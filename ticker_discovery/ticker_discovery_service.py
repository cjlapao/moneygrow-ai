from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import re, html, httpx, asyncio, time

app = FastAPI(title="MoneyGrowAI Ticker Discovery (n8n shape)")

# ----- regex + helpers -----
STOP = set(
    """
AI CEO EPS ETF GDP USD IPO YOY EBIT EBITDA FCF FED SEC CPI PPI PE EV PS GAAP ADR OTC BTC ETH USD EUR
NET FIG
""".split()
)  # add false-positive tokens you see often

CASHTAG = re.compile(r"\$([A-Z]{1,5}(?:\.[A-Z]{1,2})?)\b")
PAREN = re.compile(r"\(([A-Z]{1,5})(?::[A-Z]+)?\)")  # (OXY) or (RDS.A:NYSE)
UPPERC = re.compile(r"\b([A-Z]{2,5})(?:\.[A-Z]{1,2})?\b")
URL_TKR = re.compile(r"/(quote|symbol|ticker)/([A-Z]{1,5}(?:\.[A-Z]{1,2})?)")
ACTION = re.compile(r"(?i)^\s*(keep|cut|sell)\s*:\s*(.+)$")  # "Keep: NVDA, AMZN"


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(s or "")).strip()


def _split_symbols(s: str) -> List[str]:
    parts = re.split(r"[,\s;/]+", s.strip())
    out = []
    for p in parts:
        p = p.strip().upper()
        if re.fullmatch(r"[A-Z]{1,5}(?:\.[A-Z]{1,2})?", p):
            out.append(p)
    return out


# ----- models (n8n story shape) -----
class N8nComment(BaseModel):
    author: Optional[str] = None
    score: Optional[int] = 0
    created_utc: Optional[int] = 0
    body: Optional[str] = ""


class N8nStory(BaseModel):
    ok: Optional[bool] = True
    platform: Optional[str] = ""
    url: Optional[str] = ""
    title: Optional[str] = ""
    author: Optional[str] = ""
    score: Optional[int] = 0
    created_utc: Optional[int] = 0
    chars: Optional[int] = 0
    excerpt: Optional[str] = ""
    comments_excerpt: Optional[str] = ""
    comments: List[N8nComment] = []
    sentiment: Optional[float] = None
    source: Optional[str] = ""
    published_at: Optional[str] = ""
    interest: Optional[float] = None
    combined_text: Optional[str] = ""


class DiscoverReq(BaseModel):
    stories: List[N8nStory]
    verify: bool = True  # verify tickers via Yahoo search


class ResolveReq(BaseModel):
    tickers: List[str] = []
    names: List[str] = []  # company names to map → symbols


# ----- simple cache (per-process) -----
_cache: Dict[str, Dict[str, Any]] = {}
_TTL = 15 * 60  # 15 minutes


def cache_get(key: str):
    v = _cache.get(key)
    if not v:
        return None
    if time.time() - v["ts"] > _TTL:
        _cache.pop(key, None)
        return None
    return v["data"]


def cache_set(key: str, data: Any):
    _cache[key] = {"ts": time.time(), "data": data}


# ----- Yahoo search (verification/mapping) -----
async def yahoo_search(q: str) -> Dict[str, Any]:
    key = f"yh:{q}"
    hit = cache_get(key)
    if hit is not None:
        return hit
    url = "https://query1.finance.yahoo.com/v1/finance/search"
    params = {"q": q, "quotesCount": 5, "newsCount": 0}
    headers = {"User-Agent": "MoneyGrowAI/1.0"}
    async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
        r = await client.get(url, params=params, headers=headers)
        r.raise_for_status()
        data = r.json()
        cache_set(key, data)
        return data


async def verify_symbol(sym: str) -> Optional[Dict[str, Any]]:
    try:
        d = await yahoo_search(sym)
        for q in d.get("quotes", []):
            if str(q.get("symbol", "")).upper() == sym.upper():
                return {
                    "symbol": q.get("symbol"),
                    "shortname": q.get("shortname"),
                    "longname": q.get("longname"),
                    "exchange": q.get("exchDisp"),
                    "type": q.get("quoteType"),
                }
        return None
    except Exception:
        return None


async def resolve_name(name: str) -> Optional[Dict[str, Any]]:
    try:
        d = await yahoo_search(name)
        for q in d.get("quotes", []):
            if q.get("quoteType") in ("EQUITY", "ETF", "MUTUALFUND", "INDEX", "CRYPTO"):
                return {
                    "symbol": q.get("symbol"),
                    "shortname": q.get("shortname"),
                    "longname": q.get("longname"),
                    "exchange": q.get("exchDisp"),
                    "type": q.get("quoteType"),
                }
        return None
    except Exception:
        return None


# ----- core detection for one story -----
def detect_from_story(s: N8nStory) -> Dict[str, Any]:
    blocks = [s.title or "", s.excerpt or "", s.combined_text or ""]
    if s.comments:
        blocks.extend([c.body or "" for c in s.comments])
    joined = " \n ".join(_clean(b) for b in blocks)

    candidates: Dict[str, Dict[str, Any]] = {}

    def add(sym: str, reason: str):
        sym = sym.upper()
        if len(sym) > 6:  # crude guard
            return
        if sym in STOP:
            return
        if not re.fullmatch(r"[A-Z]{1,5}(?:\.[A-Z]{1,2})?", sym):
            return
        entry = candidates.get(sym) or {"symbol": sym, "reasons": set()}
        entry["reasons"].add(reason)
        candidates[sym] = entry

    # cashtags / parens / URL patterns
    for m in CASHTAG.finditer(joined):
        add(m.group(1), "cashtag")
    for m in PAREN.finditer(joined):
        add(m.group(1), "paren")
    for m in URL_TKR.finditer(joined):
        add(m.group(2), "url")

    # ACTION lists (Keep:/Cut:/Sell:)
    tallies: Dict[str, Dict[str, int]] = {}
    for raw_line in joined.split("\n"):
        m = ACTION.match(raw_line)
        if not m:
            continue
        action = m.group(1).lower()
        sym_list = _split_symbols(m.group(2))
        for sym in sym_list:
            add(sym, f"list:{action.capitalize()}")
            t = tallies.get(sym) or {"ticker": sym, "keep": 0, "cut": 0, "sell": 0}
            t[action] += 1
            tallies[sym] = t

    # ALLCAPS tokens (guarded)
    for m in UPPERC.finditer(joined):
        sym = m.group(1).upper()
        if sym not in STOP and len(sym) <= 5 and not sym.isdigit():
            add(sym, "allcaps")

    cand_list = []
    for sym, d in sorted(candidates.items(), key=lambda kv: kv[0]):
        cand_list.append({"symbol": sym, "reasons": sorted(list(d["reasons"]))})

    tallies_list = sorted(tallies.values(), key=lambda x: x["ticker"])

    return {
        "url": s.url,
        "title": s.title,
        "candidates": cand_list,
        "tallies": tallies_list,
    }


# ----- endpoints -----
@app.post("/discover_n8n")
async def discover_n8n(req: DiscoverReq) -> Dict[str, Any]:
    per_story = [detect_from_story(st) for st in req.stories]
    union = sorted({c["symbol"] for ps in per_story for c in ps["candidates"]})

    allowed = union
    verified = []
    if req.verify and union:
        res = await asyncio.gather(*[verify_symbol(sym) for sym in union])
        verified = [x for x in res if x]
        allowed = sorted({x["symbol"].upper() for x in verified})

    # attach allowed per story
    for ps in per_story:
        ps["allowed_tickers"] = sorted(
            {c["symbol"] for c in ps["candidates"] if c["symbol"] in allowed}
        )

    return {
        "per_story": per_story,
        "union_candidates": union,
        "verified": verified,
        "allowed_tickers": allowed,
    }


@app.post("/resolve")
async def resolve(req: ResolveReq) -> Dict[str, Any]:
    """
    Verify explicit tickers and map company names -> symbols.
    Returns a consolidated allowed_tickers list.
    """
    tickers = sorted({t.upper() for t in (req.tickers or [])})
    names = sorted({n.strip() for n in (req.names or []) if n and n.strip()})

    verified, mapped = [], []

    if tickers:
        vs = await asyncio.gather(*[verify_symbol(t) for t in tickers])
        verified = [x for x in vs if x]

    if names:
        ms = await asyncio.gather(*[resolve_name(n) for n in names])
        mapped = [x for x in ms if x]

    allowed = sorted({x["symbol"].upper() for x in (verified + mapped)})

    return {
        "verified": verified,  # exact-symbol matches
        "mapped": mapped,  # name->symbol matches
        "allowed_tickers": allowed,
    }


@app.get("/health")
def health():
    return {"ok": True}
