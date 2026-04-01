"""
FastAPI server for PM Intelligence signals v3.

Runs two background loops:
1. Discovery scanner (3x daily at scheduled hours)
2. Price monitor (every 2 minutes for known pairs)

API serves cached data from the price monitor.
"""

import json
import sqlite3
import threading
import time
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

from config import (
    API_HOST, API_PORT, DB_PATH,
    PRICE_MONITOR_INTERVAL, DISCOVERY_HOURS_UTC,
    DIVERGENCE_THRESHOLD,
)
from collector import (
    init_db, run_discovery, run_price_monitor,
    get_active_pairs_with_prices,
)
from signals.divergence import compute_divergence_from_pairs
from signals.calibration import calibrate_market
from signals.volume_quality import assess_quality


# ─────────────────────────────────────────────
# In-memory cache
# ─────────────────────────────────────────────

_cache = {
    "last_update": None,
    "last_discovery": None,
    "pairs": [],           # active pairs with latest prices
    "divergences": [],     # pairs with divergence > threshold
    "monitor_stats": {},
    "discovery_stats": {},
}


def _update_cache():
    """Refresh cache from DB after a price monitor run."""
    pairs = get_active_pairs_with_prices()

    divergences = []
    for p in pairs:
        if p.get("divergence") is not None and abs(p["divergence"]) >= DIVERGENCE_THRESHOLD:
            divergences.append({
                "pair_id": p["id"],
                "polymarket_question": p["pm_question"],
                "kalshi_question": p["km_question"],
                "polymarket_yes": round(p["pm_yes"], 4) if p["pm_yes"] else None,
                "kalshi_yes": round(p["km_yes"], 4) if p["km_yes"] else None,
                "divergence": round(p["divergence"], 4),
                "abs_divergence": round(abs(p["divergence"]), 4),
                "higher_on": "polymarket" if p["divergence"] > 0 else "kalshi",
                "similarity": p["similarity"],
                "pm_volume": p.get("pm_volume", 0),
                "kalshi_volume": p.get("km_volume", 0),
                "last_updated": p.get("last_price_update"),
            })

    divergences.sort(key=lambda x: x["abs_divergence"], reverse=True)

    _cache["pairs"] = pairs
    _cache["divergences"] = divergences
    _cache["last_update"] = datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────
# Background threads
# ─────────────────────────────────────────────

def _bg_price_monitor():
    """Runs every PRICE_MONITOR_INTERVAL seconds."""
    while True:
        try:
            stats = run_price_monitor()
            _cache["monitor_stats"] = stats
            _update_cache()
        except Exception as e:
            print(f"[MONITOR] Error: {e}")
        time.sleep(PRICE_MONITOR_INTERVAL)


def _bg_discovery_scheduler():
    """Runs discovery at scheduled hours. Checks every 60 seconds."""
    last_discovery_hour = None

    while True:
        now = datetime.now(timezone.utc)
        current_hour = now.hour

        if current_hour in DISCOVERY_HOURS_UTC and current_hour != last_discovery_hour:
            try:
                stats = run_discovery()
                _cache["discovery_stats"] = stats
                _cache["last_discovery"] = now.isoformat()
                last_discovery_hour = current_hour
                _update_cache()
            except Exception as e:
                print(f"[DISCOVERY] Error: {e}")

        # Reset the guard when the hour changes
        if current_hour not in DISCOVERY_HOURS_UTC:
            last_discovery_hour = None

        time.sleep(60)  # Check every minute


# ─────────────────────────────────────────────
# FastAPI app
# ─────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()

    # Run initial discovery on startup
    print("[STARTUP] Running initial discovery scan...")
    try:
        stats = run_discovery()
        _cache["discovery_stats"] = stats
        _cache["last_discovery"] = datetime.now(timezone.utc).isoformat()
    except Exception as e:
        print(f"[STARTUP] Discovery error: {e}")

    # Run initial price monitor
    try:
        monitor_stats = run_price_monitor()
        _cache["monitor_stats"] = monitor_stats
    except Exception as e:
        print(f"[STARTUP] Monitor error: {e}")

    _update_cache()
    print(f"[STARTUP] Ready. {len(_cache['pairs'])} active pairs, {len(_cache['divergences'])} divergences")

    # Start background threads
    t_monitor = threading.Thread(target=_bg_price_monitor, daemon=True)
    t_discovery = threading.Thread(target=_bg_discovery_scheduler, daemon=True)
    t_monitor.start()
    t_discovery.start()

    yield


app = FastAPI(title="PM Intelligence Feed", version="3.0", lifespan=lifespan)


def _stale():
    if not _cache["last_update"]:
        return True
    age = (datetime.now(timezone.utc) - datetime.fromisoformat(_cache["last_update"])).total_seconds()
    return age > 300


# ─────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────

@app.get("/health")
def health():
    return {
        "status": "ok",
        "last_update": _cache["last_update"],
        "last_discovery": _cache["last_discovery"],
        "active_pairs": len(_cache["pairs"]),
        "divergences": len(_cache["divergences"]),
        "monitor_stats": _cache["monitor_stats"],
        "discovery_schedule_utc": DISCOVERY_HOURS_UTC,
    }


@app.get("/divergences")
def divergences(limit: int = Query(20)):
    return {
        "timestamp": _cache["last_update"],
        "stale": _stale(),
        "divergences": _cache["divergences"][:limit],
        "total": len(_cache["divergences"]),
    }


@app.get("/matched-markets")
def matched_markets(status: str = Query("active")):
    """All cross-platform matched pairs with latest prices."""
    pairs = []
    for p in _cache["pairs"]:
        pairs.append({
            "pair_id": p["id"],
            "polymarket_question": p["pm_question"],
            "kalshi_question": p["km_question"],
            "similarity": p["similarity"],
            "polymarket_yes": round(p["pm_yes"], 4) if p.get("pm_yes") else None,
            "kalshi_yes": round(p["km_yes"], 4) if p.get("km_yes") else None,
            "divergence": round(p["divergence"], 4) if p.get("divergence") else None,
            "pm_volume": p.get("pm_volume", 0),
            "kalshi_volume": p.get("km_volume", 0),
            "last_updated": p.get("last_price_update"),
        })
    return {
        "timestamp": _cache["last_update"],
        "matched_pairs": pairs,
        "total": len(pairs),
    }


@app.get("/signals")
def signals(
    signal: str = Query("all"),
    event: str = Query(""),
    limit: int = Query(20),
):
    """Combined signal endpoint for Conduit feed."""
    result = {"timestamp": _cache["last_update"], "stale": _stale()}

    if signal in ("all", "divergence"):
        divs = _cache["divergences"]
        if event:
            el = event.lower()
            divs = [d for d in divs if el in d["polymarket_question"].lower() or el in d["kalshi_question"].lower()]
        result["divergences"] = divs[:limit]
        result["divergence_count"] = len(_cache["divergences"])

    if signal in ("all", "calibrated"):
        calibrated = []
        for p in _cache["pairs"][:limit]:
            if event and event.lower() not in p["pm_question"].lower():
                continue
            entry = {
                "question": p["pm_question"],
                "similarity": p["similarity"],
            }
            if p.get("pm_yes"):
                entry["polymarket"] = calibrate_market(p["pm_yes"], p.get("pm_volume", 0), "polymarket")
            if p.get("km_yes"):
                entry["kalshi"] = calibrate_market(p["km_yes"], p.get("km_volume", 0), "kalshi")
            calibrated.append(entry)
        result["calibrated"] = calibrated

    if signal == "all":
        result["matched_count"] = len(_cache["pairs"])

    return JSONResponse(content=result)


@app.get("/track-record")
def track_record():
    """Historical accuracy of divergence signals."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row

        total_pairs = conn.execute("SELECT COUNT(*) FROM matched_pairs").fetchone()[0]
        active_pairs = conn.execute("SELECT COUNT(*) FROM matched_pairs WHERE status='active'").fetchone()[0]
        resolved_pairs = conn.execute("SELECT COUNT(*) FROM matched_pairs WHERE status='resolved'").fetchone()[0]

        div_stats = conn.execute("""
            SELECT
                COUNT(*) as total_observations,
                AVG(ABS(divergence)) as avg_divergence,
                MAX(ABS(divergence)) as max_divergence,
                MIN(timestamp) as first_observation,
                MAX(timestamp) as last_observation
            FROM divergence_history
        """).fetchone()

        res_stats = conn.execute("""
            SELECT
                COUNT(*) as total_resolved,
                SUM(CASE WHEN signal_correct = 1 THEN 1 ELSE 0 END) as correct,
                AVG(peak_divergence) as avg_peak_divergence
            FROM resolutions
        """).fetchone()

        total_resolved = res_stats["total_resolved"] or 0
        correct = res_stats["correct"] or 0
        accuracy = round(correct / total_resolved * 100, 1) if total_resolved > 0 else None

        top_divs = conn.execute("""
            SELECT mp.pm_question, mp.km_question, mp.status,
                   MAX(ABS(dh.divergence)) as peak_div,
                   COUNT(dh.id) as observations
            FROM divergence_history dh
            JOIN matched_pairs mp ON dh.matched_pair_id = mp.id
            GROUP BY dh.matched_pair_id
            ORDER BY peak_div DESC
            LIMIT 10
        """).fetchall()

        # Recent scan log
        scans = conn.execute(
            "SELECT * FROM scan_log ORDER BY timestamp DESC LIMIT 5"
        ).fetchall()

        conn.close()

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "total_pairs_ever": total_pairs,
                "active_pairs": active_pairs,
                "resolved_pairs": resolved_pairs,
                "total_resolved_with_outcome": total_resolved,
                "correct_signals": correct,
                "accuracy_pct": accuracy,
                "total_divergence_observations": div_stats["total_observations"] or 0,
                "avg_divergence": round(div_stats["avg_divergence"] or 0, 4),
                "max_divergence_seen": round(div_stats["max_divergence"] or 0, 4),
                "tracking_since": div_stats["first_observation"],
            },
            "top_divergences": [
                {
                    "polymarket": row["pm_question"],
                    "kalshi": row["km_question"],
                    "status": row["status"],
                    "peak_divergence": round(row["peak_div"], 4),
                    "observations": row["observations"],
                }
                for row in top_divs
            ],
            "recent_scans": [dict(row) for row in scans],
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/history/{pair_id}")
def pair_history(pair_id: int, limit: int = Query(100)):
    """Full divergence time series for a matched pair."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row

        pair = conn.execute("SELECT * FROM matched_pairs WHERE id=?", (pair_id,)).fetchone()
        if not pair:
            conn.close()
            return {"error": "Pair not found"}

        history = conn.execute("""
            SELECT timestamp, pm_yes, km_yes, divergence, pm_volume, km_volume
            FROM divergence_history
            WHERE matched_pair_id = ?
            ORDER BY timestamp DESC
            LIMIT ?
        """, (pair_id, limit)).fetchall()

        conn.close()

        return {
            "pair": {
                "id": pair["id"],
                "polymarket": pair["pm_question"],
                "kalshi": pair["km_question"],
                "similarity": pair["similarity"],
                "first_seen": pair["first_seen"],
                "last_seen": pair["last_seen"],
                "status": pair["status"],
            },
            "history": [dict(row) for row in history],
            "total_observations": len(history),
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/calibrate")
def calibrate(price: float = Query(...), volume: float = Query(0)):
    return calibrate_market(price, volume)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=API_HOST, port=API_PORT)
