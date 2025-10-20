package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	ver "github.com/cjlapao/common-go-version/version"
	eng "github.com/cjlapao/moneygrow-ai/internal/rules"
	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

/*
 Minimal, production-friendly bootstrap:
 - Env-driven config with sensible defaults
 - SQLite schema auto-migration on boot
 - Tiny stdlib HTTP server with JSON helpers + CORS
 - Stubs for core endpoints you’ll flesh out next

 Build/run:
   go mod init moneygrowai/memory
   go get github.com/mattn/go-sqlite3
   go build ./cmd/server && ./server

 Env (examples):
   PORT=8080
   SQLITE_PATH=./data/moneygrow.db
   START_CASH_GBP=100
   MAX_LEVERAGE=1.2
   MAX_POS_PCT=0.15
   STOP_LOSS_PCT=0.12
   TAKE_PROFIT_PCT=0.25
   EXECUTION_PRICE_MODE=next_open
   EXCLUDE_PENNY_PRICE_USD=2
   EXCLUDE_SMALLCAP_USD=300000000
*/

const (
	version = "0.2.0"
	banner  = `
███╗   ███╗ ██████╗ ███╗   ██╗███████╗██╗   ██╗ ██████╗ ██████╗  ██████╗ ██╗    ██╗     █████╗ ██╗             
████╗ ████║██╔═══██╗████╗  ██║██╔════╝╚██╗ ██╔╝██╔════╝ ██╔══██╗██╔═══██╗██║    ██║    ██╔══██╗██║             
██╔████╔██║██║   ██║██╔██╗ ██║█████╗   ╚████╔╝ ██║  ███╗██████╔╝██║   ██║██║ █╗ ██║    ███████║██║             
██║╚██╔╝██║██║   ██║██║╚██╗██║██╔══╝    ╚██╔╝  ██║   ██║██╔══██╗██║   ██║██║███╗██║    ██╔══██║██║             
██║ ╚═╝ ██║╚██████╔╝██║ ╚████║███████╗   ██║   ╚██████╔╝██║  ██║╚██████╔╝╚███╔███╔╝    ██║  ██║██║             
╚═╝     ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚══════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝ ╚═════╝  ╚══╝╚══╝     ╚═╝  ╚═╝╚═╝             
██████╗  █████╗  ██████╗██╗  ██╗███████╗███╗   ██╗██████╗     ███████╗███████╗██████╗ ██╗   ██╗███████╗██████╗ 
██╔══██╗██╔══██╗██╔════╝██║ ██╔╝██╔════╝████╗  ██║██╔══██╗    ██╔════╝██╔════╝██╔══██╗██║   ██║██╔════╝██╔══██╗
██████╔╝███████║██║     █████╔╝ █████╗  ██╔██╗ ██║██║  ██║    ███████╗█████╗  ██████╔╝██║   ██║█████╗  ██████╔╝
██╔══██╗██╔══██║██║     ██╔═██╗ ██╔══╝  ██║╚██╗██║██║  ██║    ╚════██║██╔══╝  ██╔══██╗╚██╗ ██╔╝██╔══╝  ██╔══██╗
██████╔╝██║  ██║╚██████╗██║  ██╗███████╗██║ ╚████║██████╔╝    ███████║███████╗██║  ██║ ╚████╔╝ ███████╗██║  ██║
╚═════╝ ╚═╝  ╚═╝ ╚═════╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═══╝╚═════╝     ╚══════╝╚══════╝╚═╝  ╚═╝  ╚═══╝  ╚══════╝╚═╝  ╚═╝
`
)

// ---------- Config ----------

type Config struct {
	Port         string  `json:"port"`
	DBPath       string  `json:"sqlite_path"`
	BaseCCY      string  `json:"base_ccy"`
	StartCashGBP float64 `json:"start_cash_gbp"`

	// Risk controls
	MaxLeverage        float64 `json:"max_leverage"`         // default 1.2x
	MaxPosPct          float64 `json:"max_pos_pct"`          // default 0.15
	StopLossPct        float64 `json:"stop_loss_pct"`        // default 0.12
	TakeProfitPct      float64 `json:"take_profit_pct"`      // default 0.25
	ExecutionPriceMode string  `json:"execution_price_mode"` // "next_open"

	// Market universe exclusions (applied later by the rules engine)
	ExcludePennyPriceUSD float64 `json:"exclude_penny_price_usd"`
	ExcludeSmallCapUSD   float64 `json:"exclude_smallcap_usd"`

	// Trading frictions
	SlippageBps     float64 `json:"slippage_bps"`   // default 10 bps (0.10%)
	BrokerName      string  `json:"broker_name"`    // e.g., "Paper"
	BrokerFeeBps    float64 `json:"broker_fee_bps"` // default 5 bps (0.05%)
	BrokerMinFeeGBP float64 `json:"broker_min_fee_gbp"`

	// FX provider (free)
	FXProvider   string `json:"fx_provider"`    // "exchangerate_host"
	FXAPIURL     string `json:"fx_api_url"`     // https://api.exchangerate.host/latest
	FXBase       string `json:"fx_base"`        // "GBP"
	FXSymbolsCSV string `json:"fx_symbols_csv"` // "USD,EUR"

	// Server timeouts, CORS
	ReadTimeoutSeconds  int    `json:"read_timeout_seconds"`
	WriteTimeoutSeconds int    `json:"write_timeout_seconds"`
	IdleTimeoutSeconds  int    `json:"idle_timeout_seconds"`
	AllowOriginsCSV     string `json:"allow_origins_csv"`
}

func defaultConfig() Config {
	return Config{
		Port:         envStr("PORT", "8080"),
		DBPath:       envStr("SQLITE_PATH", "./data/moneygrow.db"),
		BaseCCY:      envStr("BASE_CCY", "GBP"),
		StartCashGBP: envFloat("START_CASH_GBP", 100.0),

		MaxLeverage:        envFloat("MAX_LEVERAGE", 1.2),
		MaxPosPct:          envFloat("MAX_POS_PCT", 0.15),
		StopLossPct:        envFloat("STOP_LOSS_PCT", 0.12),
		TakeProfitPct:      envFloat("TAKE_PROFIT_PCT", 0.25),
		ExecutionPriceMode: envStr("EXECUTION_PRICE_MODE", "next_open"),

		ExcludePennyPriceUSD: envFloat("EXCLUDE_PENNY_PRICE_USD", 2.0),
		ExcludeSmallCapUSD:   envFloat("EXCLUDE_SMALLCAP_USD", 300_000_000.0),

		SlippageBps:     envFloat("SLIPPAGE_BPS", 10), // 10 bps
		BrokerName:      envStr("BROKER_NAME", "Paper"),
		BrokerFeeBps:    envFloat("BROKER_FEE_BPS", 5), // 5 bps
		BrokerMinFeeGBP: envFloat("BROKER_MIN_FEE_GBP", 0.0),

		FXProvider:   envStr("FX_PROVIDER", "exchangerate_host"),
		FXAPIURL:     envStr("FX_API_URL", "https://api.exchangerate.host/latest"),
		FXBase:       envStr("FX_BASE", "GBP"),
		FXSymbolsCSV: envStr("FX_SYMBOLS", "USD,EUR"),

		ReadTimeoutSeconds:  envInt("READ_TIMEOUT_SECONDS", 10),
		WriteTimeoutSeconds: envInt("WRITE_TIMEOUT_SECONDS", 20),
		IdleTimeoutSeconds:  envInt("IDLE_TIMEOUT_SECONDS", 60),
		AllowOriginsCSV:     envStr("ALLOW_ORIGINS", "*"),
	}
}

type App struct {
	cfg        Config
	db         *sql.DB
	httpClient *http.Client
}

func main() {
	info, err := ver.Parse(version)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	info.Author = "John Doe1"
	info.Company = "Acme Corp"
	info.Copyright = "2025 Acme Corp"
	info.Repo = "github.com/acme/product"

	showBorder := true
	ver.PrintWithOptions(banner, info, ver.BannerOptions{
		ShowBorder: &showBorder,
	})
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	cfg := defaultConfig()
	if err := os.MkdirAll(filepath.Dir(cfg.DBPath), 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		log.Fatalf("mkdir data dir: %v", err)
	}

	db, err := sql.Open("sqlite3", cfg.DBPath+"?_busy_timeout=5000&_journal_mode=WAL&_fk=1")
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL;`); err != nil {
		log.Fatalf("pragma: %v", err)
	}
	if err := applyMigrations(db); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	app := &App{
		cfg:        cfg,
		db:         db,
		httpClient: &http.Client{Timeout: 8 * time.Second},
	}
	if err := app.ensurePortfolioRow(); err != nil {
		log.Fatalf("init portfolio: %v", err)
	}

	mux := http.NewServeMux()
	// Health & meta
	mux.HandleFunc("/healthz", app.handleHealth)
	mux.HandleFunc("/v1/meta", app.handleMeta)

	// Config
	mux.HandleFunc("/v1/config", app.withCORS(app.handleConfigGetPut))

	// Signals & decisions
	mux.HandleFunc("/v1/signals/batch", app.withCORS(app.handleSignalsBatch))
	mux.HandleFunc("/v1/decisions/run", app.withCORS(app.handleDecisionsRun))

	// Portfolio & positions
	mux.HandleFunc("/v1/portfolio", app.withCORS(app.handlePortfolioGet))
	mux.HandleFunc("/v1/positions", app.withCORS(app.handlePositionsGet))

	// FX endpoints
	mux.HandleFunc("/v1/fx/refresh", app.withCORS(app.handleFXRefresh)) // POST
	mux.HandleFunc("/v1/fx/latest", app.withCORS(app.handleFXLatest))   // GET

	// Prices ingest
	mux.HandleFunc("/v1/prices/batch", app.withCORS(app.handlePricesBatch))

	// Fill at next-day open
	mux.HandleFunc("/v1/orders/fill_next_open", app.withCORS(app.handleOrdersFillNextOpen))

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      logRequests(mux),
		ReadTimeout:  time.Duration(cfg.ReadTimeoutSeconds) * time.Second,
		WriteTimeout: time.Duration(cfg.WriteTimeoutSeconds) * time.Second,
		IdleTimeout:  time.Duration(cfg.IdleTimeoutSeconds) * time.Second,
	}

	log.Printf("MoneyGrowAI memory server %s listening on :%s (DB: %s)", version, cfg.Port, cfg.DBPath)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()
	<-ctx.Done()
	log.Println("shutdown requested")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

// ============ Handlers ============

func (a *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	jsonOK(w, http.StatusOK, map[string]any{"ok": true, "version": version})
}

func (a *App) handleMeta(w http.ResponseWriter, r *http.Request) {
	resp := map[string]any{
		"version": version,
		"config":  a.cfg,
		"now":     time.Now().UTC().Format(time.RFC3339),
	}
	jsonOK(w, http.StatusOK, resp)
}

// GET /v1/config returns the in-memory effective config.
// PUT /v1/config accepts a partial JSON to update selected fields and persists them into the config table.
func (a *App) handleConfigGetPut(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		jsonOK(w, http.StatusOK, a.cfg)
		return
	case http.MethodPut:
		var patch map[string]any
		if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
			jsonErr(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		for k, v := range patch {
			switch strings.ToLower(k) {
			case "max_leverage":
				if f, ok := toFloat(v); ok {
					a.cfg.MaxLeverage = f
					a.upsertConfigKV("max_leverage", f)
				}
			case "max_pos_pct":
				if f, ok := toFloat(v); ok {
					a.cfg.MaxPosPct = f
					a.upsertConfigKV("max_pos_pct", f)
				}
			case "stop_loss_pct":
				if f, ok := toFloat(v); ok {
					a.cfg.StopLossPct = f
					a.upsertConfigKV("stop_loss_pct", f)
				}
			case "take_profit_pct":
				if f, ok := toFloat(v); ok {
					a.cfg.TakeProfitPct = f
					a.upsertConfigKV("take_profit_pct", f)
				}
			case "execution_price_mode":
				if s, ok := v.(string); ok {
					a.cfg.ExecutionPriceMode = s
					a.upsertConfigKV("execution_price_mode", s)
				}

			case "exclude_penny_price_usd":
				if f, ok := toFloat(v); ok {
					a.cfg.ExcludePennyPriceUSD = f
					a.upsertConfigKV("exclude_penny_price_usd", f)
				}
			case "exclude_smallcap_usd":
				if f, ok := toFloat(v); ok {
					a.cfg.ExcludeSmallCapUSD = f
					a.upsertConfigKV("exclude_smallcap_usd", f)
				}

			case "slippage_bps":
				if f, ok := toFloat(v); ok {
					a.cfg.SlippageBps = f
					a.upsertConfigKV("slippage_bps", f)
				}
			case "broker_name":
				if s, ok := v.(string); ok {
					a.cfg.BrokerName = s
					a.upsertConfigKV("broker_name", s)
				}
			case "broker_fee_bps":
				if f, ok := toFloat(v); ok {
					a.cfg.BrokerFeeBps = f
					a.upsertConfigKV("broker_fee_bps", f)
				}
			case "broker_min_fee_gbp":
				if f, ok := toFloat(v); ok {
					a.cfg.BrokerMinFeeGBP = f
					a.upsertConfigKV("broker_min_fee_gbp", f)
				}

			case "fx_provider":
				if s, ok := v.(string); ok {
					a.cfg.FXProvider = s
					a.upsertConfigKV("fx_provider", s)
				}
			case "fx_api_url":
				if s, ok := v.(string); ok {
					a.cfg.FXAPIURL = s
					a.upsertConfigKV("fx_api_url", s)
				}
			case "fx_base":
				if s, ok := v.(string); ok {
					a.cfg.FXBase = s
					a.upsertConfigKV("fx_base", s)
				}
			case "fx_symbols_csv":
				if s, ok := v.(string); ok {
					a.cfg.FXSymbolsCSV = s
					a.upsertConfigKV("fx_symbols_csv", s)
				}
			}
		}
		jsonOK(w, http.StatusOK, a.cfg)
	default:
		jsonErr(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// POST /v1/signals/batch
// Accepts: { "as_of_date":"YYYY-MM-DD", "model_run_id":"...", "signals":[{symbol,action,weight,confidence,risk_blob?,sources?}] }
type signalIn struct {
	Symbol     string          `json:"symbol"`
	Action     string          `json:"action"` // watch|buy_small|buy|avoid (raw input)
	Weight     float64         `json:"weight"`
	Confidence float64         `json:"confidence"`
	RiskBlob   json.RawMessage `json:"risk_blob,omitempty"`
	Sources    json.RawMessage `json:"sources,omitempty"`
}

type signalsBatchReq struct {
	AsOfDate   string     `json:"as_of_date"`
	ModelRunID string     `json:"model_run_id"`
	Signals    []signalIn `json:"signals"`
}

func (a *App) handleSignalsBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req signalsBatchReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonErr(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if req.AsOfDate == "" || req.ModelRunID == "" {
		jsonErr(w, http.StatusBadRequest, "as_of_date and model_run_id are required")
		return
	}

	tx, err := a.db.Begin()
	if err != nil {
		jsonErr(w, 500, "db begin error")
		return
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO signals (symbol, as_of_date, action, weight, confidence, risk_blob, sources, model_run_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(symbol, as_of_date, model_run_id) DO UPDATE SET
		  action=excluded.action, weight=excluded.weight, confidence=excluded.confidence, risk_blob=excluded.risk_blob, sources=excluded.sources
	`)
	if err != nil {
		jsonErr(w, 500, "prepare error")
		return
	}
	defer stmt.Close()

	var inserted int
	for _, s := range req.Signals {
		if s.Symbol == "" {
			continue
		}
		_, err := stmt.Exec(strings.ToUpper(s.Symbol), req.AsOfDate, s.Action, s.Weight, s.Confidence, nullJSON(s.RiskBlob), nullJSON(s.Sources), req.ModelRunID)
		if err != nil {
			jsonErr(w, 500, fmt.Sprintf("insert signal %s: %v", s.Symbol, err))
			return
		}
		inserted++
	}
	if err := tx.Commit(); err != nil {
		jsonErr(w, 500, "commit error")
		return
	}

	jsonOK(w, http.StatusOK, map[string]any{
		"ok": true, "inserted": inserted, "as_of_date": req.AsOfDate, "model_run_id": req.ModelRunID,
	})
}

// POST /v1/decisions/run?date=YYYY-MM-DD
// Stub for now; will evaluate yesterday->today state machine, sizing, leverage checks, and stage paper orders at next open.
func (a *App) handleDecisionsRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	date := r.URL.Query().Get("date")
	if date == "" {
		jsonErr(w, 400, "date is required (YYYY-MM-DD)")
		return
	}
	ctx := r.Context()

	// Map server config to engine config
	ec := eng.Config{
		BaseCCY:            a.cfg.BaseCCY,
		MaxLeverage:        a.cfg.MaxLeverage,
		MaxPosPct:          a.cfg.MaxPosPct,
		StopLossPct:        a.cfg.StopLossPct,
		TakeProfitPct:      a.cfg.TakeProfitPct,
		ExecutionPriceMode: a.cfg.ExecutionPriceMode,
		SlippageBps:        a.cfg.SlippageBps,
		BrokerName:         a.cfg.BrokerName,
		BrokerFeeBps:       a.cfg.BrokerFeeBps,
		BrokerMinFeeGBP:    a.cfg.BrokerMinFeeGBP,
		FXBase:             a.cfg.FXBase,
	}

	out, err := eng.Run(ctx, a.db, ec, date)
	if err != nil {
		jsonErr(w, 500, fmt.Sprintf("decision run error: %v", err))
		return
	}
	jsonOK(w, 200, map[string]any{
		"ok":              true,
		"date":            out.Date,
		"recommendations": out.Recommendations,
		"staged_orders":   out.Orders,
	})
}

// GET /v1/portfolio
func (a *App) handlePortfolioGet(w http.ResponseWriter, r *http.Request) {
	row := a.db.QueryRow(`SELECT id, base_ccy, cash_gbp, equity_gbp, nav_gbp, leverage, dd_peak_nav_gbp, dd_max, updated_at FROM portfolio WHERE id=1`)
	var id int64
	var base string
	var cash, equity, nav, lev, peak, ddmax float64
	var updated string
	if err := row.Scan(&id, &base, &cash, &equity, &nav, &lev, &peak, &ddmax, &updated); err != nil {
		jsonErr(w, 500, "portfolio scan error")
		return
	}
	jsonOK(w, 200, map[string]any{
		"id": id, "base_ccy": base, "cash_gbp": cash, "equity_gbp": equity, "nav_gbp": nav,
		"leverage": lev, "dd_peak_nav_gbp": peak, "dd_max": ddmax, "updated_at": updated,
	})
}

// GET /v1/positions
func (a *App) handlePositionsGet(w http.ResponseWriter, r *http.Request) {
	rows, err := a.db.Query(`
		SELECT id, symbol, qty, avg_cost_ccy, ccy, fx_to_gbp, opened_at, status
		FROM positions WHERE status='open' ORDER BY opened_at ASC
	`)
	if err != nil {
		jsonErr(w, 500, "positions query error")
		return
	}
	defer rows.Close()

	type pos struct {
		ID         int64   `json:"id"`
		Symbol     string  `json:"symbol"`
		Qty        float64 `json:"qty"`
		AvgCostCCY float64 `json:"avg_cost_ccy"`
		CCY        string  `json:"ccy"`
		FXToGBP    float64 `json:"fx_to_gbp"`
		OpenedAt   string  `json:"opened_at"`
		Status     string  `json:"status"`
	}
	var out []pos
	for rows.Next() {
		var p pos
		if err := rows.Scan(&p.ID, &p.Symbol, &p.Qty, &p.AvgCostCCY, &p.CCY, &p.FXToGBP, &p.OpenedAt, &p.Status); err != nil {
			jsonErr(w, 500, "positions scan error")
			return
		}
		out = append(out, p)
	}
	jsonOK(w, 200, map[string]any{"positions": out})
}

// ----- FX endpoints -----

// POST /v1/fx/refresh?symbols=USD,EUR
// Fetches latest base->quote rates from configured provider and upserts into fx_rates.
func (a *App) handleFXRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	s := r.URL.Query().Get("symbols")
	if s == "" {
		s = a.cfg.FXSymbolsCSV
	}
	symbols := splitCSV(s)
	if len(symbols) == 0 {
		jsonErr(w, 400, "no symbols provided")
		return
	}
	rates, err := a.fetchFXRates(a.cfg.FXProvider, a.cfg.FXAPIURL, a.cfg.FXBase, symbols)
	if err != nil {
		jsonErr(w, 502, fmt.Sprintf("fx fetch error: %v", err))
		return
	}
	if err := a.upsertFXRates(a.cfg.FXBase, rates, a.cfg.FXProvider); err != nil {
		jsonErr(w, 500, fmt.Sprintf("fx store error: %v", err))
		return
	}
	jsonOK(w, 200, map[string]any{"ok": true, "base": a.cfg.FXBase, "provider": a.cfg.FXProvider, "rates": rates})
}

// GET /v1/fx/latest?symbols=USD,EUR
func (a *App) handleFXLatest(w http.ResponseWriter, r *http.Request) {
	s := r.URL.Query().Get("symbols")
	var rows *sql.Rows
	var err error
	if s == "" {
		rows, err = a.db.Query(`SELECT base, quote, rate, provider, ts FROM fx_rates WHERE base=? ORDER BY quote ASC`, a.cfg.FXBase)
	} else {
		symbols := splitCSV(s)
		qMarks := strings.Repeat("?,", len(symbols))
		qMarks = strings.TrimRight(qMarks, ",")
		args := make([]any, 0, len(symbols)+1)
		args = append(args, a.cfg.FXBase)
		for _, q := range symbols {
			args = append(args, strings.ToUpper(q))
		}
		query := fmt.Sprintf(`SELECT base, quote, rate, provider, ts FROM fx_rates WHERE base=? AND quote IN (%s)`, qMarks)
		rows, err = a.db.Query(query, args...)
	}
	if err != nil {
		jsonErr(w, 500, "fx query error")
		return
	}
	defer rows.Close()
	type rec struct {
		Base, Quote, Provider, TS string
		Rate                      float64
	}
	out := map[string]rec{}
	for rows.Next() {
		var rec rec
		if err := rows.Scan(&rec.Base, &rec.Quote, &rec.Rate, &rec.Provider, &rec.TS); err != nil {
			jsonErr(w, 500, "fx scan error")
			return
		}
		out[rec.Quote] = rec
	}
	jsonOK(w, 200, map[string]any{"base": a.cfg.FXBase, "rates": out})
}

// Provider adapter (v1: exchangerate.host)
func (a *App) fetchFXRates(provider, apiURL, base string, symbols []string) (map[string]float64, error) {
	switch strings.ToLower(provider) {
	case "exchangerate_host", "exchangeratehost", "exchangerate":
		return a.fetchFXFromExchangeRateHost(apiURL, base, symbols)
	default:
		return nil, fmt.Errorf("unknown FX provider: %s", provider)
	}
}

func (a *App) fetchFXFromExchangeRateHost(apiURL, base string, symbols []string) (map[string]float64, error) {
	u, err := url.Parse(apiURL)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("base", strings.ToUpper(base))
	q.Set("symbols", strings.Join(toUpper(symbols), ","))
	u.RawQuery = q.Encode()

	req, _ := http.NewRequest(http.MethodGet, u.String(), nil)
	req.Header.Set("User-Agent", "MoneyGrowAI/FX (Go)")
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var payload struct {
		Base  string             `json:"base"`
		Rates map[string]float64 `json:"rates"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if len(payload.Rates) == 0 {
		return nil, fmt.Errorf("no rates in response")
	}
	return payload.Rates, nil
}

func (a *App) upsertFXRates(base string, rates map[string]float64, provider string) error {
	tx, err := a.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`
		INSERT INTO fx_rates (base, quote, rate, provider, ts)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(base, quote) DO UPDATE SET
		  rate=excluded.rate, provider=excluded.provider, ts=excluded.ts
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	now := time.Now().UTC().Format(time.RFC3339)
	for q, r := range rates {
		if _, err := stmt.Exec(strings.ToUpper(base), strings.ToUpper(q), r, provider, now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// POST /v1/prices/batch
// Body: { "as_of_date":"YYYY-MM-DD", "prices":[{"symbol":"AAPL","open_ccy":234.56,"ccy":"USD"}, ...] }
type priceIn struct {
	Symbol  string  `json:"symbol"`
	OpenCCY float64 `json:"open_ccy"`
	CCY     string  `json:"ccy"` // default USD
}
type pricesBatchReq struct {
	AsOfDate string    `json:"as_of_date"`
	Prices   []priceIn `json:"prices"`
}

func (a *App) handlePricesBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req pricesBatchReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonErr(w, 400, "invalid JSON")
		return
	}
	if req.AsOfDate == "" || len(req.Prices) == 0 {
		jsonErr(w, 400, "as_of_date and prices are required")
		return
	}
	tx, err := a.db.Begin()
	if err != nil {
		jsonErr(w, 500, "db begin error")
		return
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`
		INSERT INTO prices (symbol, as_of_date, open_ccy, ccy)
		VALUES (?, ?, ?, COALESCE(?, 'USD'))
		ON CONFLICT(symbol, as_of_date) DO UPDATE SET open_ccy=excluded.open_ccy, ccy=excluded.ccy
	`)
	if err != nil {
		jsonErr(w, 500, "prepare error")
		return
	}
	defer stmt.Close()
	var up int
	for _, p := range req.Prices {
		if p.Symbol == "" || p.OpenCCY <= 0 {
			continue
		}
		if _, err := stmt.Exec(strings.ToUpper(p.Symbol), req.AsOfDate, p.OpenCCY, strings.ToUpper(strings.TrimSpace(p.CCY))); err != nil {
			jsonErr(w, 500, fmt.Sprintf("upsert %s: %v", p.Symbol, err))
			return
		}
		up++
	}
	if err := tx.Commit(); err != nil {
		jsonErr(w, 500, "commit error")
		return
	}
	jsonOK(w, 200, map[string]any{"ok": true, "as_of_date": req.AsOfDate, "upserted": up})
}

// POST /v1/orders/fill_next_open?date=YYYY-MM-DD
// Fills all orders with status='new' using prices.open_ccy for that date.
// Applies slippage (bps) and broker fees (bps, min GBP). Uses FX at FILL TIME.
func (a *App) handleOrdersFillNextOpen(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	execDate := r.URL.Query().Get("date")
	if execDate == "" {
		jsonErr(w, 400, "date is required (YYYY-MM-DD)")
		return
	}

	// Load all NEW orders
	rows, err := a.db.Query(`SELECT id, symbol, side, qty, notional_ccy, ccy FROM orders WHERE status='new' ORDER BY id ASC`)
	if err != nil {
		jsonErr(w, 500, "orders query error")
		return
	}
	defer rows.Close()

	type orderRow struct {
		ID                int64
		Symbol, Side, CCY string
		Qty, NotionalCCY  float64
	}
	var orders []orderRow
	for rows.Next() {
		var o orderRow
		if err := rows.Scan(&o.ID, &o.Symbol, &o.Side, &o.Qty, &o.NotionalCCY, &o.CCY); err != nil {
			jsonErr(w, 500, "orders scan error")
			return
		}
		orders = append(orders, o)
	}
	if len(orders) == 0 {
		jsonOK(w, 200, map[string]any{"ok": true, "filled": 0})
		return
	}

	// Load all needed opens into a map
	symbols := make([]string, 0, len(orders))
	need := map[string]struct{}{}
	for _, o := range orders {
		if _, ok := need[o.Symbol]; !ok {
			need[o.Symbol] = struct{}{}
			symbols = append(symbols, o.Symbol)
		}
	}
	qmarks := strings.TrimRight(strings.Repeat("?,", len(symbols)), ",")
	args := make([]any, 0, len(symbols)+1)
	args = append(args, execDate)
	for _, s := range symbols {
		args = append(args, s)
	}
	priceQ := fmt.Sprintf(`SELECT symbol, open_ccy, ccy FROM prices WHERE as_of_date=? AND symbol IN (%s)`, qmarks)
	pr, err := a.db.Query(priceQ, args...)
	if err != nil {
		jsonErr(w, 500, "prices query error")
		return
	}
	defer pr.Close()
	type px struct {
		Open float64
		CCY  string
	}
	prices := map[string]px{}
	for pr.Next() {
		var sym, ccy string
		var open float64
		if err := pr.Scan(&sym, &open, &ccy); err != nil {
			jsonErr(w, 500, "prices scan error")
			return
		}
		prices[sym] = px{Open: open, CCY: ccy}
	}
	// Verify all have prices
	for _, o := range orders {
		if _, ok := prices[o.Symbol]; !ok {
			jsonErr(w, 400, fmt.Sprintf("missing open price for %s on %s", o.Symbol, execDate))
			return
		}
	}

	// FX: base GBP → instrument CCY (e.g., USD) at fill time
	// We store FX to GBP on the order/position as fill factor
	fxFor := func(ccy string) (gbpToQuote float64, quoteToGBP float64, err error) {
		if strings.ToUpper(ccy) == strings.ToUpper(a.cfg.BaseCCY) {
			return 1.0, 1.0, nil
		}
		row := a.db.QueryRow(`SELECT rate FROM fx_rates WHERE base=? AND quote=? ORDER BY ts DESC LIMIT 1`,
			strings.ToUpper(a.cfg.FXBase), strings.ToUpper(ccy))
		var rate float64
		if err := row.Scan(&rate); err != nil {
			return 0, 0, fmt.Errorf("no FX rate %s->%s", a.cfg.FXBase, ccy)
		}
		return rate, 1.0 / rate, nil
	}

	slippage := a.cfg.SlippageBps / 10_000.0
	feeBps := a.cfg.BrokerFeeBps / 10_000.0
	feeMin := a.cfg.BrokerMinFeeGBP

	tx, err := a.db.Begin()
	if err != nil {
		jsonErr(w, 500, "db begin error")
		return
	}
	defer tx.Rollback()

	// Statements
	updOrder, _ := tx.Prepare(`UPDATE orders SET price_ccy=?, qty=?, status='filled', filled_at=? WHERE id=?`)
	defer updOrder.Close()
	updPosNew, _ := tx.Prepare(`INSERT INTO positions (symbol, qty, avg_cost_ccy, ccy, fx_to_gbp, opened_at, status) VALUES (?, ?, ?, ?, ?, ?, 'open')`)
	defer updPosNew.Close()
	updPosAdd, _ := tx.Prepare(`UPDATE positions SET qty = qty + ?, avg_cost_ccy = ((avg_cost_ccy*qty_before) + (?*?)) / (qty_before + ?), opened_at=opened_at WHERE id=?`)
	// avg_cost update uses a trick: we’ll compute qty_before via a select per-row below
	defer updPosAdd.Close()
	getPos, _ := tx.Prepare(`SELECT id, qty, avg_cost_ccy, ccy, fx_to_gbp FROM positions WHERE symbol=? AND status='open' LIMIT 1`)
	defer getPos.Close()
	closePos, _ := tx.Prepare(`UPDATE positions SET qty=0, status='closed', closed_at=? WHERE id=?`)
	defer closePos.Close()
	updQtyPos, _ := tx.Prepare(`UPDATE positions SET qty = qty - ? WHERE id=?`)
	defer updQtyPos.Close()
	insLedger, _ := tx.Prepare(`INSERT INTO ledger (ts, type, ref_id, symbol, debit_gbp, credit_gbp, balance_after_gbp, note) VALUES (?,?,?,?,?,?,?,?)`)
	defer insLedger.Close()
	updPortfolio, _ := tx.Prepare(`UPDATE portfolio SET cash_gbp=?, equity_gbp=?, nav_gbp=?, leverage=?, dd_peak_nav_gbp=?, dd_max=?, updated_at=? WHERE id=1`)
	defer updPortfolio.Close()

	// Load portfolio
	var cashGBP, equityGBP, navGBP, lev, peak, ddmax float64
	if err := tx.QueryRow(`SELECT cash_gbp, equity_gbp, nav_gbp, leverage, dd_peak_nav_gbp, dd_max FROM portfolio WHERE id=1`).Scan(&cashGBP, &equityGBP, &navGBP, &lev, &peak, &ddmax); err != nil {
		jsonErr(w, 500, "portfolio load error")
		return
	}

	type fillResp struct {
		OrderID      int64   `json:"order_id"`
		Symbol       string  `json:"symbol"`
		Side         string  `json:"side"`
		FillPriceCCY float64 `json:"fill_price_ccy"`
		Qty          float64 `json:"qty"`
		CCY          string  `json:"ccy"`
		FeeGBP       float64 `json:"fee_gbp"`
	}
	var filled []fillResp

	now := time.Now().UTC().Format(time.RFC3339)

	for _, o := range orders {
		p := prices[o.Symbol]
		_, quoteToGBP, err := fxFor(p.CCY)
		if err != nil {
			jsonErr(w, 400, err.Error())
			return
		}

		fillPrice := p.Open
		if o.Side == "buy" {
			fillPrice *= (1.0 + slippage)
		} else if o.Side == "sell" {
			fillPrice *= (1.0 - slippage)
		}

		switch o.Side {
		case "buy":
			// Compute qty from staged notional
			if o.NotionalCCY <= 0 {
				jsonErr(w, 400, fmt.Sprintf("buy order %d missing notional", o.ID))
				return
			}
			qty := o.NotionalCCY / fillPrice // fractional ok

			// Cash impact (GBP)
			notionalGBP := o.NotionalCCY * quoteToGBP
			feeGBP := max(feeMin, notionalGBP*feeBps)
			cashGBP -= (notionalGBP + feeGBP)
			equityGBP += notionalGBP // mark new position at cost at fill; NAV decreases only by fee

			// Position upsert
			var posID int64
			var prevQty, prevAvg, prevFX float64
			var posCCY string
			if err := getPos.QueryRow(o.Symbol).Scan(&posID, &prevQty, &prevAvg, &posCCY, &prevFX); err == nil {
				// update avg cost with qty_before trick
				// compute new avg_cost: ((prevAvg*prevQty) + (fillPrice*qty)) / (prevQty+qty)
				_, err = tx.Exec(`UPDATE positions SET avg_cost_ccy = ((avg_cost_ccy*qty) + (?*?)) / (qty + ?), qty = qty + ?, fx_to_gbp=?, status='open' WHERE id=?`,
					fillPrice, qty, qty, qty, quoteToGBP, posID)
				if err != nil {
					jsonErr(w, 500, "update position error")
					return
				}
			} else {
				// insert new
				if _, err := updPosNew.Exec(o.Symbol, qty, fillPrice, p.CCY, quoteToGBP, now); err != nil {
					jsonErr(w, 500, "insert position error")
					return
				}
			}

			// Mark order filled
			if _, err := updOrder.Exec(fillPrice, qty, now, o.ID); err != nil {
				jsonErr(w, 500, "update order error")
				return
			}

			// Ledger
			if _, err := insLedger.Exec(now, "order_fill", o.ID, o.Symbol, notionalGBP+feeGBP, 0.0, cashGBP, fmt.Sprintf("BUY %s qty=%.6f @ %.4f %s, fee=%.4f GBP", o.Symbol, qty, fillPrice, p.CCY, feeGBP)); err != nil {
				jsonErr(w, 500, "ledger buy error")
				return
			}

			filled = append(filled, fillResp{OrderID: o.ID, Symbol: o.Symbol, Side: o.Side, FillPriceCCY: fillPrice, Qty: qty, CCY: p.CCY, FeeGBP: feeGBP})

		case "sell":
			// qty is pre-set (trim or close)
			if o.Qty <= 0 {
				jsonErr(w, 400, fmt.Sprintf("sell order %d qty<=0", o.ID))
				return
			}

			// Proceeds (GBP)
			proceedsGBP := (o.Qty * fillPrice) * quoteToGBP
			feeGBP := max(feeMin, proceedsGBP*feeBps)
			cashGBP += (proceedsGBP - feeGBP)
			equityGBP -= (o.Qty * fillPrice) * quoteToGBP // reduce marked equity at fill price; realized P&L floats into NAV via cash diff

			// Update position
			var posID int64
			var prevQty float64
			if err := getPos.QueryRow(o.Symbol).Scan(&posID, &prevQty, new(any), new(any), new(any)); err != nil {
				jsonErr(w, 400, fmt.Sprintf("no open position for %s to sell", o.Symbol))
				return
			}
			if o.Qty >= prevQty-1e-9 {
				if _, err := closePos.Exec(now, posID); err != nil {
					jsonErr(w, 500, "close position error")
					return
				}
			} else {
				if _, err := updQtyPos.Exec(o.Qty, posID); err != nil {
					jsonErr(w, 500, "reduce position error")
					return
				}
			}

			// Mark order filled
			if _, err := updOrder.Exec(fillPrice, o.Qty, now, o.ID); err != nil {
				jsonErr(w, 500, "update order error")
				return
			}

			// Ledger
			if _, err := insLedger.Exec(now, "order_fill", o.ID, o.Symbol, 0.0, proceedsGBP-feeGBP, cashGBP, fmt.Sprintf("SELL %s qty=%.6f @ %.4f %s, fee=%.4f GBP", o.Symbol, o.Qty, fillPrice, p.CCY, feeGBP)); err != nil {
				jsonErr(w, 500, "ledger sell error")
				return
			}

			filled = append(filled, fillResp{OrderID: o.ID, Symbol: o.Symbol, Side: o.Side, FillPriceCCY: fillPrice, Qty: o.Qty, CCY: p.CCY, FeeGBP: feeGBP})
		}
	}

	// Recompute NAV/leverage crudely: NAV = cash + equity; leverage = gross_exposure / NAV
	navGBP = cashGBP + equityGBP
	if navGBP <= 0 {
		navGBP = 0.000001
	}
	grossExposure := equityGBP // long-only for now
	lev = grossExposure / navGBP

	// Drawdown
	if navGBP > peak {
		peak = navGBP
	}
	dd := 0.0
	if peak > 0 {
		dd = (navGBP - peak) / peak // negative number
	}
	if dd < ddmax {
		ddmax = dd
	}

	if _, err := updPortfolio.Exec(cashGBP, equityGBP, navGBP, lev, peak, ddmax, now); err != nil {
		jsonErr(w, 500, "update portfolio error")
		return
	}

	if err := tx.Commit(); err != nil {
		jsonErr(w, 500, "commit error")
		return
	}

	jsonOK(w, 200, map[string]any{
		"ok":        true,
		"date":      execDate,
		"filled":    filled,
		"portfolio": map[string]any{"cash_gbp": cashGBP, "equity_gbp": equityGBP, "nav_gbp": navGBP, "leverage": lev},
	})
}

// ----- Signals/Decisions/Portfolio/Positions handlers remain as in your previous file -----

// ---------- Helpers ----------

func (a *App) withCORS(next http.HandlerFunc) http.HandlerFunc {
	allowed := strings.Split(a.cfg.AllowOriginsCSV, ",")
	for i := range allowed {
		allowed[i] = strings.TrimSpace(allowed[i])
	}
	return func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if a.cfg.AllowOriginsCSV == "*" {
			w.Header().Set("Access-Control-Allow-Origin", "*")
		} else if origin != "" && contains(allowed, origin) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PUT,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next(w, r)
	}
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func jsonOK(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func jsonErr(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": msg})
}

func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case string:
		f, err := strconv.ParseFloat(t, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func contains(arr []string, s string) bool {
	for _, v := range arr {
		if v == s {
			return true
		}
	}
	return false
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func toUpper(ss []string) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = strings.ToUpper(strings.TrimSpace(s))
	}
	return out
}

func nullJSON(b json.RawMessage) any {
	if len(b) == 0 || string(b) == "null" {
		return nil
	}
	return string(b)
}

// ============ Helpers ============

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t0 := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s %v", r.RemoteAddr, r.Method, r.URL.Path, time.Since(t0))
	})
}

// Ensure portfolio row exists and initialize cash if first run.
func (a *App) ensurePortfolioRow() error {
	var count int
	if err := a.db.QueryRow(`SELECT COUNT(*) FROM portfolio WHERE id=1`).Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		_, err := a.db.Exec(`
			INSERT INTO portfolio (id, base_ccy, cash_gbp, equity_gbp, nav_gbp, leverage, dd_peak_nav_gbp, dd_max, updated_at)
			VALUES (1, ?, ?, 0, ?, 0, ?, 0, ?)
		`, a.cfg.BaseCCY, a.cfg.StartCashGBP, a.cfg.StartCashGBP, a.cfg.StartCashGBP, time.Now().UTC().Format(time.RFC3339))
		return err
	}
	return nil
}

func (a *App) upsertConfigKV(key string, val any) {
	b, _ := json.Marshal(val)
	_, _ = a.db.Exec(`INSERT INTO config (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, string(b))
}

// ============ Schema Migration ============

func applyMigrations(db *sql.DB) error {
	schema := `
CREATE TABLE IF NOT EXISTS tickers (
  symbol TEXT PRIMARY KEY,
  name   TEXT,
  exchange TEXT,
  created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS signals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL REFERENCES tickers(symbol) ON DELETE CASCADE ON UPDATE CASCADE,
  as_of_date TEXT NOT NULL,
  action TEXT NOT NULL,
  weight REAL NOT NULL,
  confidence REAL NOT NULL,
  risk_blob TEXT,
  sources  TEXT,
  model_run_id TEXT NOT NULL,
  created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(symbol, as_of_date, model_run_id)
);

CREATE TABLE IF NOT EXISTS recommendations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL,
  as_of_date TEXT NOT NULL,
  stance TEXT NOT NULL,
  reasons TEXT,
  inputs_hash TEXT,
  created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(symbol, as_of_date)
);

CREATE TABLE IF NOT EXISTS positions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL,
  qty REAL NOT NULL,
  avg_cost_ccy REAL NOT NULL,
  ccy TEXT NOT NULL DEFAULT 'USD',
  fx_to_gbp REAL NOT NULL DEFAULT 1.0,
  opened_at TEXT NOT NULL,
  closed_at TEXT,
  status TEXT NOT NULL DEFAULT 'open'
);

CREATE TABLE IF NOT EXISTS orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  qty REAL NOT NULL,
  price_ccy REAL NOT NULL,
  notional_ccy REAL NOT NULL,
  ccy TEXT NOT NULL DEFAULT 'USD',
  fx_to_gbp REAL NOT NULL DEFAULT 1.0,
  type TEXT NOT NULL DEFAULT 'market',
  status TEXT NOT NULL DEFAULT 'new',
  decision_id INTEGER,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  filled_at TEXT
);

CREATE TABLE IF NOT EXISTS portfolio (
  id INTEGER PRIMARY KEY,
  base_ccy TEXT NOT NULL DEFAULT 'GBP',
  cash_gbp REAL NOT NULL DEFAULT 0.0,
  equity_gbp REAL NOT NULL DEFAULT 0.0,
  nav_gbp REAL NOT NULL DEFAULT 0.0,
  leverage REAL NOT NULL DEFAULT 0.0,
  dd_peak_nav_gbp REAL NOT NULL DEFAULT 0.0,
  dd_max REAL NOT NULL DEFAULT 0.0,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ledger (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  type TEXT NOT NULL,
  ref_id INTEGER,
  symbol TEXT,
  debit_gbp REAL NOT NULL DEFAULT 0.0,
  credit_gbp REAL NOT NULL DEFAULT 0.0,
  balance_after_gbp REAL NOT NULL DEFAULT 0.0,
  note TEXT
);

CREATE TABLE IF NOT EXISTS snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  as_of_date TEXT NOT NULL,
  nav_gbp REAL NOT NULL,
  cash_gbp REAL NOT NULL,
  equity_gbp REAL NOT NULL,
  positions TEXT,
  recs TEXT,
  inputs_hash TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  UNIQUE(as_of_date)
);

CREATE TABLE IF NOT EXISTS config (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fx_rates (
  base TEXT NOT NULL,
  quote TEXT NOT NULL,
  rate REAL NOT NULL,
  provider TEXT NOT NULL,
  ts TEXT NOT NULL,
  PRIMARY KEY (base, quote)
);

CREATE TABLE IF NOT EXISTS prices (
  symbol TEXT NOT NULL,
  as_of_date TEXT NOT NULL,       -- YYYY-MM-DD (execution date)
  open_ccy REAL NOT NULL,         -- open price in instrument CCY
  ccy TEXT NOT NULL DEFAULT 'USD',
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  PRIMARY KEY (symbol, as_of_date)
);
`
	_, err := db.Exec(schema)
	return err
}

// ============ Env helpers ============

func envStr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envInt(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}
