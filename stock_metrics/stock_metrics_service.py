from fastapi import FastAPI
from pydantic import BaseModel, Field
from typing import List, Dict, Any
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import math

app = FastAPI(title="MoneyGrowAI Price & Risk Metrics")


# ---------------- helpers ----------------
def rsi(series: pd.Series, period: int = 14) -> float:
    delta = series.diff()
    up = delta.clip(lower=0).rolling(period).mean()
    down = -delta.clip(upper=0).rolling(period).mean()
    rs = up / (down.replace(0, np.nan))
    out = 100 - (100 / (1 + rs))
    return float(out.iloc[-1])


def max_drawdown(close: pd.Series) -> float:
    roll_max = close.cummax()
    dd = close / roll_max - 1.0
    return float(dd.min())


def annualized_vol(returns: pd.Series) -> float:
    return float(returns.std() * np.sqrt(252))


def sharpe_1y(returns: pd.Series, rf: float = 0.02) -> float:
    # rf is annual risk-free (e.g., 0.02 = 2%)
    mu = returns.mean() * 252
    sigma = returns.std() * np.sqrt(252)
    if sigma == 0 or np.isnan(sigma):
        return 0.0
    return float((mu - rf) / sigma)


def cagr(close: pd.Series, periods_per_year: int = 252) -> float:
    if close.size < 2:
        return 0.0
    start = float(close.iloc[0])
    end = float(close.iloc[-1])
    yrs = close.size / periods_per_year
    if start <= 0 or yrs <= 0:
        return 0.0
    return float((end / start) ** (1 / yrs) - 1)


def pct_return(close: pd.Series, days: int) -> float:
    if len(close) <= days:
        return 0.0
    return float(close.iloc[-1] / close.iloc[-days - 1] - 1)


def gain_ratio(returns: pd.Series, days: int = 90) -> float:
    r = returns.dropna().iloc[-days:]
    if r.empty:
        return 0.0
    ups = (r > 0).sum()
    return float(ups / len(r))


def zscore_price(close: pd.Series, window: int = 200) -> float:
    if len(close) < window:
        return 0.0
    ma = close.rolling(window).mean()
    sd = close.rolling(window).std()
    z = (close - ma) / sd.replace(0, np.nan)
    return float(z.iloc[-1])


def bubble_score(close: pd.Series) -> float:
    """0..1 heuristic:
    - z > 0 scaled via sigmoid
    - RSI high
    - momentum acceleration (1m > 3m > 6m returns)
    - near 52w high
    """
    if len(close) < 252:
        return 0.0
    # components
    z = max(0.0, zscore_price(close, 200))  # ignore negative z
    z_comp = 1 / (1 + math.exp(-z))  # sigmoid ~ (0.5..1)

    rsi_val = rsi(close, 14)
    rsi_comp = max(0.0, (rsi_val - 50) / 50)  # 0 at 50, 1 near 100

    r1m = pct_return(close, 21)
    r3m = pct_return(close, 63)
    r6m = pct_return(close, 126)
    accel = 1.0 if (r1m > r3m > r6m > 0) else 0.0

    hi_52w = float(close.iloc[-252:].max())
    dist_hi = 0.0 if hi_52w == 0 else close.iloc[-1] / hi_52w  # 0..1+
    dist_comp = max(0.0, min(1.0, dist_hi))  # cap at 1

    score = 0.5 * z_comp + 0.2 * rsi_comp + 0.2 * accel + 0.1 * dist_comp
    return float(max(0.0, min(1.0, score)))


# ---------------- models ----------------
class MetricsReq(BaseModel):
    tickers: List[str] = Field(..., example=["NVDA", "OXY", "AAPL"])
    period: str = Field("2y", description="yfinance period string: 6mo,1y,2y,5y,max")
    risk_free_rate: float = Field(0.02, description="Annual RF (e.g., 0.02 = 2%)")


# ---------------- endpoints ----------------
@app.get("/health")
def health():
    return {"ok": True}


@app.post("/metrics")
def metrics(req: MetricsReq) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    data = yf.download(
        req.tickers,
        period=req.period,
        interval="1d",
        auto_adjust=True,
        progress=False,
        group_by="ticker",
    )
    # Normalize yfinance output shapes for 1/Many tickers
    tickers = [t.upper() for t in req.tickers]

    for t in tickers:
        try:
            if isinstance(data.columns, pd.MultiIndex):
                df = data[t]["Close"].dropna().to_frame(name="Close")
            else:
                # single ticker comes flat
                df = data["Close"].dropna().to_frame(name="Close")

            if df.empty:
                out[t] = {"ok": False, "error": "no_data"}
                continue

            close = df["Close"]
            returns = close.pct_change()

            # current & change
            curr = float(close.iloc[-1])
            prev = float(close.iloc[-2]) if len(close) > 1 else curr
            day_chg = (curr / prev - 1.0) if prev != 0 else 0.0

            # metrics
            m = {
                "ok": True,
                "current_price": curr,
                "day_change_pct": day_chg,
                "ret_1w": pct_return(close, 5),
                "ret_1m": pct_return(close, 21),
                "ret_3m": pct_return(close, 63),
                "ret_6m": pct_return(close, 126),
                "ret_1y": pct_return(close, 252),
                "cagr_all": cagr(close),
                "vol_30d": annualized_vol(returns.iloc[-30:]),
                "vol_1y": annualized_vol(returns.iloc[-252:]),
                "sharpe_1y": sharpe_1y(returns.iloc[-252:], rf=req.risk_free_rate),
                "max_dd_1y": max_drawdown(close.iloc[-252:]),
                "rsi_14": rsi(close, 14),
                "dist_to_52w_high": (
                    0.0
                    if close.iloc[-252:].max() == 0
                    else curr / float(close.iloc[-252:].max())
                )
                - 1.0,
                "gain_ratio_90d": gain_ratio(returns, 90),
                "bubble_score": bubble_score(close),
                "last_date": str(close.index[-1].date()),
            }
            out[t] = m
        except Exception as e:
            out[t] = {"ok": False, "error": str(e)}

    return {"as_of": datetime.utcnow().isoformat() + "Z", "metrics": out}
