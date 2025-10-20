package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// Minimal config view copied from server config (only what we need here)
type Config struct {
	BaseCCY            string
	MaxLeverage        float64
	MaxPosPct          float64
	StopLossPct        float64
	TakeProfitPct      float64
	ExecutionPriceMode string // "next_open"
	SlippageBps        float64
	BrokerName         string
	BrokerFeeBps       float64
	BrokerMinFeeGBP    float64
	FXBase             string // "GBP"
}

// DB model slices
type Signal struct {
	Symbol     string
	Action     string
	Weight     float64
	Confidence float64
	Risk       map[string]float64 // bubble_score, rsi_14, sharpe_1y, vol_30d, max_dd_1y, etc.
}

type Position struct {
	ID         int64
	Symbol     string
	Qty        float64
	AvgCostCCY float64
	CCY        string
	FXToGBP    float64
	Status     string
}

type Recommendation struct {
	Symbol   string
	AsOfDate string
	Stance   string
	Reasons  map[string]any
}

type OrderDraft struct {
	Symbol      string
	Side        string // buy|sell|trim
	Qty         float64
	PriceCCY    float64 // 0 at staging; filled later
	NotionalCCY float64 // for buys we stage notional (in CCY); for sells it will be 0 (computed from qty at fill)
	CCY         string  // default "USD" for v1
	FXToGBP     float64 // factor to convert CCY->GBP at staging time
	Type        string  // market
	Status      string  // new
}

// Public entry point
type Result struct {
	Date            string           `json:"date"`
	Recommendations []Recommendation `json:"recommendations"`
	Orders          []OrderDraft     `json:"staged_orders"`
}

func Run(ctx context.Context, db *sql.DB, cfg Config, date string) (Result, error) {
	var res Result
	res.Date = date

	navGBP, err := loadNAV(db)
	if err != nil {
		return res, err
	}

	signals, err := loadSignals(db, date)
	if err != nil {
		return res, err
	}

	prevStance, err := loadPrevStances(db, date)
	if err != nil {
		return res, err
	}

	openPos, err := loadOpenPositions(db)
	if err != nil {
		return res, err
	}

	// Latest GBP->USD FX factor (USD per GBP)
	gbpToUSD, err := latestFXRate(db, cfg.FXBase, "USD")
	if err != nil {
		// Default to 1.25 USD/GBP if missing (safe-ish), and caller can refresh FX before running
		gbpToUSD = 1.25
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return res, err
	}
	defer tx.Rollback()

	upsertRec, err := tx.Prepare(`
		INSERT INTO recommendations (symbol, as_of_date, stance, reasons, inputs_hash)
		VALUES (?, ?, ?, ?, NULL)
		ON CONFLICT(symbol, as_of_date) DO UPDATE SET stance=excluded.stance, reasons=excluded.reasons
	`)
	if err != nil {
		return res, err
	}
	defer upsertRec.Close()

	insOrder, err := tx.Prepare(`
		INSERT INTO orders (symbol, side, qty, price_ccy, notional_ccy, ccy, fx_to_gbp, type, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, 'market', 'new')
	`)
	if err != nil {
		return res, err
	}
	defer insOrder.Close()

	recs := make([]Recommendation, 0, len(signals))
	orders := make([]OrderDraft, 0, len(signals))

	for _, s := range signals {
		prev := strings.ToLower(prevStance[s.Symbol])
		pos := openPos[s.Symbol] // may be zero value

		stance, reason := resolveStance(s, prev, pos)

		// Write recommendation
		reasonsJSON, _ := json.Marshal(reason)
		if _, err := upsertRec.Exec(s.Symbol, date, stance, string(reasonsJSON)); err != nil {
			return res, fmt.Errorf("upsert rec %s: %w", s.Symbol, err)
		}
		recs = append(recs, Recommendation{
			Symbol: s.Symbol, AsOfDate: date, Stance: stance, Reasons: reason,
		})

		// Stage orders (buys: notional only; sells: explicit qty)
		switch stance {
		case "buy", "buy_small":
			// sizing target (GBP)
			targetGBP := min(s.Weight*navGBP, cfg.MaxPosPct*navGBP)
			if targetGBP <= 0 {
				break
			}

			// convert GBP -> USD notional
			notionalUSD := targetGBP * gbpToUSD

			// FXToGBP factor to convert USD -> GBP later
			fxToGBP := 1.0 / gbpToUSD

			od := OrderDraft{
				Symbol: s.Symbol, Side: "buy",
				Qty: 0, PriceCCY: 0,
				NotionalCCY: notionalUSD, CCY: "USD",
				FXToGBP: fxToGBP, Type: "market", Status: "new",
			}
			if _, err := insOrder.Exec(od.Symbol, od.Side, od.Qty, od.PriceCCY, od.NotionalCCY, od.CCY, od.FXToGBP); err != nil {
				return res, fmt.Errorf("insert order buy %s: %w", s.Symbol, err)
			}
			orders = append(orders, od)

		case "sell":
			if pos.ID == 0 || pos.Qty <= 0 {
				break
			}
			od := OrderDraft{
				Symbol: s.Symbol, Side: "sell",
				Qty: pos.Qty, PriceCCY: 0, NotionalCCY: 0,
				CCY: pos.CCY, FXToGBP: pos.FXToGBP, Type: "market", Status: "new",
			}
			if _, err := insOrder.Exec(od.Symbol, od.Side, od.Qty, od.PriceCCY, od.NotionalCCY, od.CCY, od.FXToGBP); err != nil {
				return res, fmt.Errorf("insert order sell %s: %w", s.Symbol, err)
			}
			orders = append(orders, od)

		case "trim":
			if pos.ID == 0 || pos.Qty <= 0 {
				break
			}
			trimQty := pos.Qty * 0.25 // default; later make configurable
			if trimQty <= 0 {
				break
			}
			od := OrderDraft{
				Symbol: s.Symbol, Side: "sell",
				Qty: trimQty, PriceCCY: 0, NotionalCCY: 0,
				CCY: pos.CCY, FXToGBP: pos.FXToGBP, Type: "market", Status: "new",
			}
			if _, err := insOrder.Exec(od.Symbol, od.Side, od.Qty, od.PriceCCY, od.NotionalCCY, od.CCY, od.FXToGBP); err != nil {
				return res, fmt.Errorf("insert order trim %s: %w", s.Symbol, err)
			}
			orders = append(orders, od)
		}
	}

	if err := tx.Commit(); err != nil {
		return res, err
	}
	res.Recommendations, res.Orders = recs, orders
	return res, nil
}

// ---------- helpers ----------

func loadNAV(db *sql.DB) (float64, error) {
	row := db.QueryRow(`SELECT nav_gbp FROM portfolio WHERE id=1`)
	var nav float64
	return nav, row.Scan(&nav)
}

func loadSignals(db *sql.DB, date string) ([]Signal, error) {
	rows, err := db.Query(`
		SELECT symbol, action, weight, confidence, COALESCE(risk_blob,'{}')
		FROM signals WHERE as_of_date = ?`, date)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Signal
	for rows.Next() {
		var s Signal
		var riskRaw string
		if err := rows.Scan(&s.Symbol, &s.Action, &s.Weight, &s.Confidence, &riskRaw); err != nil {
			return nil, err
		}
		s.Risk = map[string]float64{}
		_ = json.Unmarshal([]byte(riskRaw), &s.Risk)
		out = append(out, s)
	}
	return out, nil
}

func loadPrevStances(db *sql.DB, date string) (map[string]string, error) {
	rows, err := db.Query(`
		WITH prev AS (
		  SELECT symbol, MAX(as_of_date) AS d
		  FROM recommendations
		  WHERE as_of_date < ?
		  GROUP BY symbol
		)
		SELECT r.symbol, r.stance
		FROM recommendations r
		JOIN prev p ON r.symbol = p.symbol AND r.as_of_date = p.d
	`, date)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := map[string]string{}
	for rows.Next() {
		var sym, stance string
		if err := rows.Scan(&sym, &stance); err != nil {
			return nil, err
		}
		m[sym] = stance
	}
	return m, nil
}

func loadOpenPositions(db *sql.DB) (map[string]Position, error) {
	rows, err := db.Query(`SELECT id, symbol, qty, avg_cost_ccy, ccy, fx_to_gbp, status FROM positions WHERE status='open'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := map[string]Position{}
	for rows.Next() {
		var p Position
		if err := rows.Scan(&p.ID, &p.Symbol, &p.Qty, &p.AvgCostCCY, &p.CCY, &p.FXToGBP, &p.Status); err != nil {
			return nil, err
		}
		m[p.Symbol] = p
	}
	return m, nil
}

func latestFXRate(db *sql.DB, base, quote string) (float64, error) {
	row := db.QueryRow(`SELECT rate FROM fx_rates WHERE base=? AND quote=? ORDER BY ts DESC LIMIT 1`, strings.ToUpper(base), strings.ToUpper(quote))
	var rate float64
	if err := row.Scan(&rate); err != nil {
		return 0, err
	}
	return rate, nil
}

// Decision logic, conservative by default
func resolveStance(s Signal, prev string, pos Position) (string, map[string]any) {
	a := strings.ToLower(s.Action)

	// Risk flags from risk blob (if provided)
	bubble := s.Risk["bubble_score"]
	rsi := s.Risk["rsi_14"]
	sharpe := s.Risk["sharpe_1y"]
	vol30 := s.Risk["vol_30d"]
	maxDD := s.Risk["max_dd_1y"]

	overheated := bubble >= 0.75 || (rsi >= 75 && sharpe < 0.8)
	tooVolatile := vol30 >= 0.60 || maxDD <= -0.45

	reason := map[string]any{
		"prev":       prev,
		"sig_action": a,
		"weight":     s.Weight,
		"risk": map[string]float64{
			"bubble_score": bubble, "rsi_14": rsi, "sharpe_1y": sharpe, "vol_30d": vol30, "max_dd_1y": maxDD,
		},
	}

	// Hard avoid overrides
	if a == "avoid" {
		if pos.ID != 0 && pos.Qty > 0 {
			reason["decision"] = "sell on avoid"
			return "sell", reason
		}
		reason["decision"] = "avoid (no position)"
		return "avoid", reason
	}

	// Overheated / too volatile → watch/trim
	if overheated || tooVolatile {
		if pos.ID != 0 && pos.Qty > 0 {
			reason["decision"] = "trim on risk"
			return "trim", reason
		}
		reason["decision"] = "watch on risk"
		return "watch", reason
	}

	// If we have a position already
	if pos.ID != 0 && pos.Qty > 0 {
		switch a {
		case "buy", "buy_small":
			reason["decision"] = "hold/add-lite"
			return "hold", reason // we keep it simple; add-on sizing can be another rule
		case "watch":
			reason["decision"] = "hold/watch"
			return "hold", reason
		default:
			reason["decision"] = "hold"
			return "hold", reason
		}
	}

	// No position yet
	switch a {
	case "buy":
		reason["decision"] = "init buy"
		return "buy", reason
	case "buy_small":
		reason["decision"] = "init buy_small"
		return "buy_small", reason
	case "watch":
		// gentle nudge: repeat watch -> buy_small (if no risk flags)
		if prev == "watch" {
			reason["decision"] = "escalate watch->buy_small"
			return "buy_small", reason
		}
		reason["decision"] = "watch (no position)"
		return "watch", reason
	default:
		reason["decision"] = "watch (fallback)"
		return "watch", reason
	}
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
