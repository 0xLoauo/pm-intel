"""FastAPI server for PM Intelligence signals."""

import json
import threading
import time
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

from config import API_HOST, API_PORT, COLLECT_INTERVAL_SECONDS
from collector import collect_once, init_db
from signals.divergence import compute_divergence
from signals.calibration import calibrate_market
from signals.volume_quality import assess_quality


_cache = {
    "last_update": None,
    "pm_markets": [],
    "km_markets": [],
    "matches": [],
    "divergences": [],
    "stats": {},
}


def _refresh():
    try:
        stats, pm, km, matches = collect_once()
        divergences = compute_divergence(matches)
        _cache.update({
            "last_update": datetime.now(timezone.utc).isoformat(),
            "pm_markets": pm,
            "km_markets": km,
            "matches": matches,
            "divergences": divergences,
            "stats": stats,
        })
    except Exception as e:
        print(f"[API] Refresh error: {e}")


def _bg_collector():
    while True:
        _refresh()
        time.sleep(COLLECT_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    _refresh()
    t = threading.Thread(target=_bg_collector, daemon=True)
    t.start()
    yield


app = FastAPI(title="PM Intelligence Feed", version="1.0", lifespan=lifespan)


def _stale():
    if not _cache["last_update"]:
        return True
    age = (datetime.now(timezone.utc) - datetime.fromisoformat(_cache["last_update"])).total_seconds()
    return age > 300


@app.get("/health")
def health():
    return {"status": "ok", "last_update": _cache["last_update"], "stats": _cache["stats"]}


@app.get("/signals")
def signals(
    signal: str = Query("all"),
    event: str = Query(""),
    limit: int = Query(20),
):
    result = {"timestamp": _cache["last_update"], "stale": _stale()}

    if signal in ("all", "divergence"):
        divs = _cache["divergences"]
        if event:
            el = event.lower()
            divs = [d for d in divs if el in d["polymarket_question"].lower() or el in d["kalshi_title"].lower()]
        result["divergences"] = divs[:limit]
        result["divergence_count"] = len(_cache["divergences"])

    if signal in ("all", "calibrated"):
        calibrated = []
        for m in _cache["matches"][:limit]:
            pm = m["polymarket"]
            km = m["kalshi"]
            prices = pm.get("outcomePrices")
            if isinstance(prices, str):
                try:
                    prices = json.loads(prices)
                except:
                    prices = []
            pm_yes = float(prices[0]) if isinstance(prices, list) and prices else None
            km_yes = float(km.get("yes_ask", 0) or 0) or None

            if event and event.lower() not in pm.get("question", "").lower():
                continue

            entry = {
                "question": pm.get("question", ""),
                "similarity": m["similarity"],
                "matched_entities": m.get("matched_entities", []),
            }
            if pm_yes:
                entry["polymarket"] = calibrate_market(pm_yes, float(pm.get("volume", 0) or 0), "polymarket")
            if km_yes:
                entry["kalshi"] = calibrate_market(km_yes, float(km.get("volume", 0) or 0), "kalshi")
            calibrated.append(entry)
        result["calibrated"] = calibrated

    if signal in ("all", "quality"):
        qlist = []
        for pm in _cache["pm_markets"][:50]:
            q = assess_quality(pm, "polymarket")
            q["question"] = pm.get("question", "")
            if event and event.lower() not in q["question"].lower():
                continue
            qlist.append(q)
        result["quality"] = sorted(qlist, key=lambda x: x["quality_score"])[:limit]

    if signal == "all":
        result["matched_count"] = len(_cache["matches"])
        result["polymarket_count"] = len(_cache["pm_markets"])
        result["kalshi_count"] = len(_cache["km_markets"])

    return JSONResponse(content=result)


@app.get("/divergences")
def divergences(limit: int = Query(20)):
    return {
        "timestamp": _cache["last_update"],
        "stale": _stale(),
        "divergences": _cache["divergences"][:limit],
        "total": len(_cache["divergences"]),
    }


@app.get("/calibrate")
def calibrate(price: float = Query(...), volume: float = Query(0)):
    return calibrate_market(price, volume)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=API_HOST, port=API_PORT)
