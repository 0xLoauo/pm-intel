"""Data collector — fetches from Polymarket + Kalshi, runs matching + signals."""

import httpx
import sqlite3
import json
import time
from datetime import datetime, timezone
from config import (
    POLYMARKET_GAMMA_API, KALSHI_API, DB_PATH,
    COLLECT_INTERVAL_SECONDS, MATCH_SIMILARITY_THRESHOLD,
)
from matching.event_matcher import match_events


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            platform TEXT NOT NULL,
            platform_id TEXT NOT NULL,
            question TEXT NOT NULL,
            yes_price REAL,
            volume REAL,
            end_date TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_snap_ts ON snapshots(timestamp);
        CREATE INDEX IF NOT EXISTS idx_snap_pid ON snapshots(platform, platform_id);
    ''')
    conn.execute("PRAGMA journal_mode=WAL")
    conn.commit()
    return conn


def fetch_polymarket(limit=500):
    markets = []
    for offset in range(0, limit, 100):
        try:
            r = httpx.get(f"{POLYMARKET_GAMMA_API}/markets",
                         params={"active": "true", "closed": "false", "limit": 100, "offset": offset},
                         timeout=30)
            r.raise_for_status()
            batch = r.json()
            markets.extend(batch)
            if len(batch) < 100:
                break
        except Exception as e:
            print(f"[PM] Error at offset {offset}: {e}")
            break
        time.sleep(0.3)
    return markets


def fetch_kalshi_events():
    """Fetch Kalshi events with nested priced markets."""
    markets = []
    try:
        r = httpx.get(f"{KALSHI_API}/events",
                     params={"limit": 100, "status": "open", "with_nested_markets": "true"},
                     timeout=30)
        r.raise_for_status()
        events = r.json().get("events", [])
        for e in events:
            for m in e.get("markets", []):
                price = float(m.get("last_price_dollars", "0") or "0")
                if price > 0:
                    m["title"] = m.get("title", e.get("title", ""))
                    m["yes_ask"] = price
                    m["_category"] = e.get("category", "")
                    m["volume"] = float(m.get("volume_fp", "0") or "0")
                    markets.append(m)
    except Exception as e:
        print(f"[Kalshi] Error: {e}")
    return markets


def collect_once():
    conn = init_db()
    now = datetime.now(timezone.utc)
    now_str = now.strftime("%H:%M:%S")
    now_iso = now.isoformat()

    pm_markets = fetch_polymarket(500)
    km_markets = fetch_kalshi_events()

    # Store PM snapshots
    for m in pm_markets:
        p = m.get("outcomePrices")
        if isinstance(p, str):
            try:
                p = json.loads(p)
            except:
                p = []
        yes_price = float(p[0]) if isinstance(p, list) and p else 0
        try:
            conn.execute(
                "INSERT INTO snapshots (timestamp, platform, platform_id, question, yes_price, volume, end_date) VALUES (?,?,?,?,?,?,?)",
                (now_iso, "polymarket", str(m.get("conditionId", m.get("id", ""))),
                 m.get("question", ""), yes_price, float(m.get("volume", 0) or 0), m.get("endDate", ""))
            )
        except:
            pass

    # Store Kalshi snapshots
    for m in km_markets:
        try:
            conn.execute(
                "INSERT INTO snapshots (timestamp, platform, platform_id, question, yes_price, volume, end_date) VALUES (?,?,?,?,?,?,?)",
                (now_iso, "kalshi", m.get("ticker", ""), m.get("title", ""),
                 float(m.get("yes_ask", 0)), m.get("volume", 0), m.get("close_time", ""))
            )
        except:
            pass

    matches = match_events(pm_markets, km_markets, MATCH_SIMILARITY_THRESHOLD)

    conn.commit()
    conn.close()

    stats = {"polymarket": len(pm_markets), "kalshi": len(km_markets), "matched": len(matches)}
    print(f"[{now_str}] {stats}")
    return stats, pm_markets, km_markets, matches


if __name__ == "__main__":
    print("PM Intel Collector starting...")
    init_db()
    while True:
        try:
            collect_once()
        except Exception as e:
            print(f"Error: {e}")
        time.sleep(COLLECT_INTERVAL_SECONDS)
