from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from urllib.parse import urlparse
import httpx, html, re, trafilatura
from trafilatura.settings import use_config
from typing import List, Dict

app = FastAPI(title="MoneyGrowAI Article Extractor v2")


class Req(BaseModel):
    url: str


def clean_text(s: str) -> str:
    if not s:
        return ""
    s = html.unescape(s)
    # strip common boilerplate and excessive whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def reddit_json_url(url: str) -> str:
    """
    Accepts:
      https://www.reddit.com/r/stocks/comments/1o0wixo/title/
      https://reddit.com/r/stocks/comments/1o0wixo/title/
    Returns:
      https://www.reddit.com/comments/1o0wixo.json?sort=top&depth=1&limit=50
    """
    parts = urlparse(url)
    path = parts.path
    m = re.search(r"/comments/([A-Za-z0-9]+)/", path)
    if not m:
        # fallback: if already a /comments/<id> without trailing slash
        m = re.search(r"/comments/([A-Za-z0-9]+)", path)
    if not m:
        raise ValueError("Cannot parse Reddit post id from URL")
    post_id = m.group(1)
    return f"https://www.reddit.com/comments/{post_id}.json?sort=top&depth=1&limit=50"


async def fetch(url: str, headers: Dict[str, str] = None) -> httpx.Response:
    h = {"User-Agent": "MoneyGrowAI/1.0 (+https://carloslapao.com)"}
    if headers:
        h.update(headers)
    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        r = await client.get(url, headers=h)
        return r


def extract_reddit(json_payload) -> Dict:
    """
    reddit returns a list of 2 JSON docs:
      [0] = post listing, [1] = comments listing
    """
    post = json_payload[0]["data"]["children"][0]["data"]
    title = clean_text(post.get("title", ""))
    selftext = clean_text(post.get("selftext", ""))
    url = "https://www.reddit.com" + post.get("permalink", "")
    author = post.get("author", "")
    created_utc = post.get("created_utc", 0)
    score = post.get("score", 0)

    # comments
    comments_raw = json_payload[1]["data"]["children"]
    comments = []
    for c in comments_raw:
        if c.get("kind") != "t1":  # t1 = comment
            continue
        d = c.get("data", {})
        body = clean_text(d.get("body", ""))
        if not body or body in ("[deleted]", "[removed]"):
            continue
        comments.append(
            {
                "author": d.get("author", ""),
                "score": d.get("score", 0),
                "created_utc": d.get("created_utc", 0),
                "body": body[:2000],  # cap a single comment length
            }
        )

    # sort by score desc, keep top N
    comments.sort(key=lambda x: x.get("score", 0), reverse=True)
    top_n = comments[:20]

    # build article-like excerpts
    comments_excerpt = "\n".join([f"- {c['body']}" for c in top_n])[:4000]
    # final excerpt prioritizes post body + selected comments
    parts = []
    if selftext:
        parts.append(selftext)
    if comments_excerpt:
        parts.append("\nTop comments:\n" + comments_excerpt)
    excerpt = ("\n\n".join(parts) or title)[:5000]

    return {
        "ok": True,
        "platform": "reddit",
        "url": url,
        "title": title,
        "author": author,
        "score": score,
        "created_utc": created_utc,
        "chars": len(excerpt),
        "excerpt": excerpt,
        "comments_excerpt": comments_excerpt,
        "comments": top_n,
    }


def extract_html(url: str, html_text: str) -> Dict:
    cfg = use_config()
    cfg.set("DEFAULT", "EXTRACTION_TIMEOUT", "0")
    text = trafilatura.extract(
        html_text,
        config=cfg,
        include_links=False,
        include_comments=False,
        no_fallback=True,
        favor_precision=True,
    )
    if not text:
        # relaxed fallback
        text = trafilatura.extract(html_text, config=cfg, include_formatting=True) or ""
    text = (text or "").strip()
    excerpt = text[:5000]
    return {"ok": True, "platform": "web", "chars": len(excerpt), "excerpt": excerpt}


@app.post("/extract")
async def extract(req: Req):
    u = req.url.strip()
    host = urlparse(u).hostname or ""
    try:
        if "reddit.com" in host:
            r = await fetch(reddit_json_url(u))
            if r.status_code >= 400:
                raise HTTPException(
                    status_code=r.status_code,
                    detail=f"Reddit fetch failed: {r.text[:200]}",
                )
            data = r.json()
            return extract_reddit(data)

        # generic HTML path
        r = await fetch(u)
        if r.status_code >= 400:
            raise HTTPException(status_code=r.status_code, detail="Fetch failed")
        return extract_html(u, r.text)

    except Exception as e:
        return {
            "ok": False,
            "url": u,
            "error": str(e),
            "excerpt": "",
            "comments_excerpt": "",
            "comments": [],
        }
