"""
Data collector v3 — split into discovery + price monitor.

Discovery (3x daily):
  - Fetches ALL markets from both platforms
  - Runs full matching to find new cross-platform pairs
  - Checks for resolved/closed markets
  - Heavy but infrequent

Price monitor (every 2 min):
  - Fetches prices ONLY for known matched pairs
  - Updates divergence history
  - Lightweight and fast
"""

import httpx
import sqlite3
import json
import time
from datetime import datetime, timezone, timedelta
from config import (
    POLYMARKET_GAMMA_API, KALSHI_API, DB_PATH,
    MATCH_SIMILARITY_THRESHOLD, DIVERGENCE_THRESHOLD,
)
from matching.event_matcher import match_events


# ─────────────────────────────────────────────
# Database
# ─────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS matched_pairs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            pm_platform_id TEXT NOT NULL,
            pm_numeric_id TEXT NOT NULL DEFAULT '',
            km_platform_id TEXT NOT NULL,
            pm_question TEXT NOT NULL,
            km_question TEXT NOT NULL,
            similarity REAL NOT NULL,
            status TEXT DEFAULT 'active',
            UNIQUE(pm_platform_id, km_platform_id)
        );
        CREATE INDEX IF NOT EXISTS idx_mp_status ON matched_pairs(status);

        CREATE TABLE IF NOT EXISTS divergence_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            matched_pair_id INTEGER NOT NULL REFERENCES matched_pairs(id),
            timestamp TEXT NOT NULL,
            pm_yes REAL,
            km_yes REAL,
            divergence REAL,
            pm_volume REAL,
            km_volume REAL
        );
        CREATE INDEX IF NOT EXISTS idx_dh_pair ON divergence_history(matched_pair_id);
        CREATE INDEX IF NOT EXISTS idx_dh_ts ON divergence_history(timestamp);

        CREATE TABLE IF NOT EXISTS resolutions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            matched_pair_id INTEGER NOT NULL REFERENCES matched_pairs(id),
            resolved_at TEXT NOT NULL,
            pm_outcome TEXT,
            km_outcome TEXT,
            divergence_at_detection REAL,
            peak_divergence REAL,
            signal_correct INTEGER,
            UNIQUE(matched_pair_id)
        );

        -- Track when the last discovery scan ran
        CREATE TABLE IF NOT EXISTS scan_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_type TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            pm_total INTEGER,
            km_total INTEGER,
            new_pairs INTEGER,
            resolved_pairs INTEGER,
            active_pairs INTEGER
        );
    ''')
    conn.execute("PRAGMA journal_mode=WAL")
    conn.commit()
    return conn


# ─────────────────────────────────────────────
# Platform fetchers
# ─────────────────────────────────────────────

def fetch_all_polymarket(limit=500):
    """Fetch all active Polymarket markets. Used in discovery only."""
    markets = []
    for offset in range(0, limit, 100):
        try:
            r = httpx.get(
                f"{POLYMARKET_GAMMA_API}/markets",
                params={"active": "true", "closed": "false", "limit": 100, "offset": offset},
                timeout=30,
            )
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


def fetch_all_kalshi():
    """Fetch ALL Kalshi events with pagination. Used in discovery only."""
    markets = []
    cursor = None
    page = 0

    while True:
        try:
            params = {"limit": 200, "status": "open", "with_nested_markets": "true"}
            if cursor:
                params["cursor"] = cursor

            r = httpx.get(f"{KALSHI_API}/events", params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            events = data.get("events", [])

            for e in events:
                for m in e.get("markets", []):
                    price = float(m.get("last_price_dollars", "0") or "0")
                    if price > 0:
                        m["title"] = m.get("title", e.get("title", ""))
                        m["yes_ask"] = price
                        m["_category"] = e.get("category", "")
                        m["volume"] = float(m.get("volume_fp", "0") or "0")
                        markets.append(m)

            cursor = data.get("cursor")
            page += 1
            if not cursor or not events or page >= 20:
                break
            time.sleep(0.3)
        except Exception as e:
            print(f"[Kalshi] Error on page {page}: {e}")
            break

    return markets


def fetch_pm_market_price(numeric_id: str) -> dict | None:
    """Fetch current price for a single Polymarket market by numeric ID."""
    try:
        r = httpx.get(
            f"{POLYMARKET_GAMMA_API}/markets/{numeric_id}",
            timeout=15,
        )
        r.raise_for_status()
        m = r.json()
        if m:
            prices = m.get("outcomePrices")
            if isinstance(prices, str):
                prices = json.loads(prices)
            yes_price = float(prices[0]) if isinstance(prices, list) and prices else None
            volume = float(m.get("volume", 0) or 0)
            active = m.get("active", True)
            return {"yes_price": yes_price, "volume": volume, "active": active, "raw": m}
    except Exception:
        pass
    return None


def fetch_km_market_price(ticker: str) -> dict | None:
    """Fetch current price for a single Kalshi market."""
    try:
        r = httpx.get(f"{KALSHI_API}/markets/{ticker}", timeout=15)
        r.raise_for_status()
        data = r.json()
        m = data.get("market", data)
        price = float(m.get("last_price_dollars", "0") or "0")
        volume = float(m.get("volume_fp", "0") or "0")
        status = m.get("status", "open")
        active = status == "open" or status == "active"
        result = m.get("result", "")
        return {
            "yes_price": price if price > 0 else None,
            "volume": volume,
            "active": active,
            "result": result,
            "raw": m,
        }
    except Exception:
        pass
    return None


# ─────────────────────────────────────────────
# Price extraction helpers
# ─────────────────────────────────────────────

def _extract_pm_yes(market: dict) -> float | None:
    """Extract YES price from a Polymarket market dict."""
    prices = market.get("outcomePrices")
    if isinstance(prices, str):
        try:
            prices = json.loads(prices)
        except:
            return None
    if isinstance(prices, list) and prices:
        return float(prices[0])
    return None


def _extract_km_yes(market: dict) -> float | None:
    """Extract YES price from a Kalshi market dict."""
    yes_ask = market.get("yes_ask")
    if yes_ask is not None:
        val = float(yes_ask)
        return val / 100 if val > 1 else val
    return None


# ─────────────────────────────────────────────
# Discovery scan (3x daily)
# ─────────────────────────────────────────────

def run_discovery():
    """
    Full discovery scan:
    1. Fetch all markets from both platforms
    2. Run matching to find new cross-platform pairs
    3. Check for resolved/closed markets among existing pairs
    4. Update matched_pairs table
    """
    print(f"\n{'='*60}")
    print(f"[DISCOVERY] Starting full scan at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")

    conn = init_db()
    now_iso = datetime.now(timezone.utc).isoformat()

    # Step 1: Fetch everything
    print("[DISCOVERY] Fetching Polymarket markets...")
    pm_markets = fetch_all_polymarket(500)
    print(f"[DISCOVERY] Got {len(pm_markets)} Polymarket markets")

    print("[DISCOVERY] Fetching Kalshi markets (all pages)...")
    km_markets = fetch_all_kalshi()
    print(f"[DISCOVERY] Got {len(km_markets)} Kalshi markets")

    # Step 2: Run matching
    print("[DISCOVERY] Running matcher...")
    t0 = time.time()
    matches = match_events(pm_markets, km_markets, MATCH_SIMILARITY_THRESHOLD)
    elapsed = time.time() - t0
    print(f"[DISCOVERY] Found {len(matches)} matches in {elapsed:.1f}s")

    # Step 3: Persist new matches
    new_pairs = 0
    matched_pm_ids = set()
    matched_km_ids = set()

    for match in matches:
        pm = match["polymarket"]
        km = match["kalshi"]

        pm_id = str(pm.get("conditionId", pm.get("id", "")))
        pm_numeric = str(pm.get("id", ""))
        km_id = km.get("ticker", "")
        pm_question = pm.get("question", "")
        km_question = km.get("title", "")
        sim = match["similarity"]

        matched_pm_ids.add(pm_id)
        matched_km_ids.add(km_id)

        row = conn.execute(
            "SELECT id FROM matched_pairs WHERE pm_platform_id=? AND km_platform_id=?",
            (pm_id, km_id)
        ).fetchone()

        if row:
            conn.execute(
                "UPDATE matched_pairs SET last_seen=?, similarity=?, pm_numeric_id=?, status='active' WHERE id=?",
                (now_iso, sim, pm_numeric, row[0])
            )
        else:
            conn.execute(
                "INSERT INTO matched_pairs (first_seen, last_seen, pm_platform_id, pm_numeric_id, km_platform_id, pm_question, km_question, similarity) VALUES (?,?,?,?,?,?,?,?)",
                (now_iso, now_iso, pm_id, pm_numeric, km_id, pm_question, km_question, sim)
            )
            new_pairs += 1
            print(f"  [NEW] {pm_question[:60]} <-> {km_question[:60]}")

    # Step 4: Check for resolved markets
    # Active pairs whose platform IDs are no longer in the live feed = resolved
    resolved_count = 0
    active_pairs = conn.execute(
        "SELECT id, pm_platform_id, pm_numeric_id, km_platform_id, pm_question, km_question FROM matched_pairs WHERE status='active'"
    ).fetchall()

    # Build lookup of live market IDs
    live_pm_ids = {str(m.get("conditionId", m.get("id", ""))) for m in pm_markets}
    live_km_ids = {m.get("ticker", "") for m in km_markets}

    for pair_id, pm_id, pm_numeric, km_id, pm_q, km_q in active_pairs:
        pm_gone = pm_id not in live_pm_ids
        km_gone = km_id not in live_km_ids

        if pm_gone or km_gone:
            # Market resolved or closed — try to get outcome
            pm_outcome = None
            km_outcome = None

            if pm_gone:
                # Try to fetch resolved market data
                pm_data = fetch_pm_market_price(pm_numeric) if pm_numeric else None
                if pm_data and not pm_data["active"]:
                    pm_outcome = "resolved"

            if km_gone:
                km_data = fetch_km_market_price(km_id)
                if km_data and not km_data["active"]:
                    km_outcome = km_data.get("result", "resolved")

            # Get peak divergence for this pair
            peak_row = conn.execute(
                "SELECT MAX(ABS(divergence)) FROM divergence_history WHERE matched_pair_id=?",
                (pair_id,)
            ).fetchone()
            peak_div = peak_row[0] if peak_row and peak_row[0] else 0

            # Get first divergence recorded
            first_div_row = conn.execute(
                "SELECT divergence FROM divergence_history WHERE matched_pair_id=? ORDER BY timestamp ASC LIMIT 1",
                (pair_id,)
            ).fetchone()
            first_div = first_div_row[0] if first_div_row else 0

            # Mark pair as resolved
            conn.execute(
                "UPDATE matched_pairs SET status='resolved', last_seen=? WHERE id=?",
                (now_iso, pair_id)
            )

            # Record resolution
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO resolutions (matched_pair_id, resolved_at, pm_outcome, km_outcome, divergence_at_detection, peak_divergence) VALUES (?,?,?,?,?,?)",
                    (pair_id, now_iso, pm_outcome, km_outcome, first_div, peak_div)
                )
            except Exception:
                pass

            resolved_count += 1
            print(f"  [RESOLVED] {pm_q[:50]} (pm={'gone' if pm_gone else 'live'}, km={'gone' if km_gone else 'live'})")

    # Step 5: Record initial divergences for new matches
    for match in matches:
        pm = match["polymarket"]
        km = match["kalshi"]
        pm_id = str(pm.get("conditionId", pm.get("id", "")))
        km_id = km.get("ticker", "")

        pm_yes = _extract_pm_yes(pm)
        km_yes = _extract_km_yes(km)

        if pm_yes is not None and km_yes is not None:
            row = conn.execute(
                "SELECT id FROM matched_pairs WHERE pm_platform_id=? AND km_platform_id=?",
                (pm_id, km_id)
            ).fetchone()
            if row:
                divergence = pm_yes - km_yes
                conn.execute(
                    "INSERT INTO divergence_history (matched_pair_id, timestamp, pm_yes, km_yes, divergence, pm_volume, km_volume) VALUES (?,?,?,?,?,?,?)",
                    (row[0], now_iso, pm_yes, km_yes, round(divergence, 4),
                     float(pm.get("volume", 0) or 0), km.get("volume", 0))
                )

    # Step 6: Log the scan
    final_active = conn.execute("SELECT COUNT(*) FROM matched_pairs WHERE status='active'").fetchone()[0]
    conn.execute(
        "INSERT INTO scan_log (scan_type, timestamp, pm_total, km_total, new_pairs, resolved_pairs, active_pairs) VALUES (?,?,?,?,?,?,?)",
        ("discovery", now_iso, len(pm_markets), len(km_markets), new_pairs, resolved_count, final_active)
    )

    conn.commit()
    conn.close()

    print(f"\n[DISCOVERY] Complete: {new_pairs} new, {resolved_count} resolved, {final_active} active pairs")
    print(f"{'='*60}\n")

    return {
        "pm_total": len(pm_markets),
        "km_total": len(km_markets),
        "matched": len(matches),
        "new_pairs": new_pairs,
        "resolved": resolved_count,
        "active_pairs": final_active,
    }


# ─────────────────────────────────────────────
# Price monitor (every 2 min)
# ─────────────────────────────────────────────

def run_price_monitor():
    """
    Lightweight price update for known matched pairs only.
    Fetches individual market prices instead of all 21K.
    """
    conn = init_db()
    now_iso = datetime.now(timezone.utc).isoformat()
    now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")

    # Get all active pairs
    pairs = conn.execute(
        "SELECT id, pm_platform_id, pm_numeric_id, km_platform_id, pm_question, km_question FROM matched_pairs WHERE status='active'"
    ).fetchall()

    if not pairs:
        print(f"[{now_str}] No active pairs to monitor")
        conn.close()
        return {"active_pairs": 0, "updated": 0, "divergences": 0}

    updated = 0
    divergence_count = 0

    # Batch fetch: collect all PM condition IDs and fetch in bulk
    # Polymarket Gamma API supports filtering, but for reliability fetch individually
    # with a small delay to avoid rate limiting
    for pair_id, pm_id, pm_numeric, km_id, pm_q, km_q in pairs:
        pm_data = fetch_pm_market_price(pm_numeric) if pm_numeric else None
        km_data = fetch_km_market_price(km_id)

        pm_yes = pm_data["yes_price"] if pm_data else None
        km_yes = km_data["yes_price"] if km_data else None

        if pm_yes is not None and km_yes is not None:
            divergence = pm_yes - km_yes
            pm_vol = pm_data["volume"] if pm_data else 0
            km_vol = km_data["volume"] if km_data else 0

            conn.execute(
                "INSERT INTO divergence_history (matched_pair_id, timestamp, pm_yes, km_yes, divergence, pm_volume, km_volume) VALUES (?,?,?,?,?,?,?)",
                (pair_id, now_iso, pm_yes, km_yes, round(divergence, 4), pm_vol, km_vol)
            )
            updated += 1

            if abs(divergence) >= DIVERGENCE_THRESHOLD:
                divergence_count += 1

        # Check if market has closed since last discovery
        if pm_data and not pm_data["active"]:
            conn.execute("UPDATE matched_pairs SET status='resolved', last_seen=? WHERE id=?", (now_iso, pair_id))
            print(f"  [CLOSED] {pm_q[:50]} (PM closed between discoveries)")
        elif km_data and not km_data["active"]:
            conn.execute("UPDATE matched_pairs SET status='resolved', last_seen=? WHERE id=?", (now_iso, pair_id))
            print(f"  [CLOSED] {km_q[:50]} (Kalshi closed between discoveries)")

        # Small delay to avoid hammering APIs
        time.sleep(0.1)

    conn.commit()
    conn.close()

    stats = {"active_pairs": len(pairs), "updated": updated, "divergences": divergence_count}
    print(f"[{now_str}] Monitor: {stats}")
    return stats


# ─────────────────────────────────────────────
# Combined data retrieval (for API cache)
# ─────────────────────────────────────────────

def get_active_pairs_with_prices():
    """
    Get all active matched pairs with their latest prices from DB.
    Used by the API to build the cache without re-fetching from platforms.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    pairs = conn.execute("""
        SELECT mp.id, mp.pm_platform_id, mp.km_platform_id,
               mp.pm_question, mp.km_question, mp.similarity, mp.status,
               dh.pm_yes, dh.km_yes, dh.divergence, dh.pm_volume, dh.km_volume,
               dh.timestamp as last_price_update
        FROM matched_pairs mp
        LEFT JOIN divergence_history dh ON dh.matched_pair_id = mp.id
            AND dh.timestamp = (
                SELECT MAX(dh2.timestamp)
                FROM divergence_history dh2
                WHERE dh2.matched_pair_id = mp.id
            )
        WHERE mp.status = 'active'
        ORDER BY ABS(COALESCE(dh.divergence, 0)) DESC
    """).fetchall()

    conn.close()
    return [dict(row) for row in pairs]
