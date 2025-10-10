from fastapi import FastAPI
from pydantic import BaseModel
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

app = FastAPI(title="MoneyGrowAI Sentiment (FinBERT)")

# Load once on startup
tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
# Keep CPU usage predictable
torch.set_num_threads(1)

# Optional speed-up (dynamic int8 quantization)
# model = torch.quantization.quantize_dynamic(model, {torch.nn.Linear}, dtype=torch.qint8)

LABELS = ["negative", "neutral", "positive"]


class Item(BaseModel):
    title: str
    source: str | None = None
    url: str | None = None
    published_at: str | None = None


class Batch(BaseModel):
    items: list[Item]


def score(texts: list[str]):
    enc = tokenizer(texts, return_tensors="pt", truncation=True, padding=True)
    with torch.inference_mode():
        logits = model(**enc).logits
        probs = torch.softmax(logits, dim=1)
    # sentiment in [-1,1] := P(pos) - P(neg)
    pos = probs[:, 2]
    neg = probs[:, 0]
    sent = (pos - neg).tolist()
    triples = probs.tolist()  # [neg, neu, pos]
    return sent, triples


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/analyze")
def analyze(item: Item):
    s, triples = score([item.title])
    neg, neu, pos = triples[0]
    return {
        "sentiment": s[0],
        "title": item.title,
        "source": item.source,
        "url": item.url,
        "published_at": item.published_at,
        "scores": {"negative": neg, "neutral": neu, "positive": pos},
    }


@app.post("/analyze_batch")
def analyze_batch(batch: Batch):
    s, triples = score(batch.items)
    out = []
    for i, t in enumerate(triples):
        neg, neu, pos = t
        out.append(
            {
                "sentiment": s[i],
                "title": batch.items[i].title,
                "source": batch.items[i].source,
                "url": batch.items[i].url,
                "published_at": batch.items[i].published_at,
                "scores": {"negative": neg, "neutral": neu, "positive": pos},
            }
        )
    return {"results": out}
