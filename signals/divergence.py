"""Signal 1: Cross-platform divergence detection.

v3: Works with both raw match dicts (from discovery) and DB pair dicts (from monitor).
"""

from config import DIVERGENCE_THRESHOLD


def compute_divergence(matched_events: list) -> list:
    """
    For each matched event pair (from discovery), compute price divergence.
    Returns flagged events where divergence exceeds threshold.
    """
    signals = []
    for match in matched_events:
        pm = match["polymarket"]
        km = match["kalshi"]

        pm_yes = _get_pm_yes_price(pm)
        km_yes = _get_kalshi_yes_price(km)

        if pm_yes is None or km_yes is None:
            continue

        divergence = pm_yes - km_yes
        abs_div = abs(divergence)

        if abs_div >= DIVERGENCE_THRESHOLD:
            signals.append({
                "polymarket_question": pm.get("question", ""),
                "kalshi_title": km.get("title", ""),
                "polymarket_yes": round(pm_yes, 4),
                "kalshi_yes": round(km_yes, 4),
                "divergence": round(divergence, 4),
                "abs_divergence": round(abs_div, 4),
                "higher_on": "polymarket" if divergence > 0 else "kalshi",
                "similarity": match["similarity"],
                "pm_volume": pm.get("volume", 0),
                "kalshi_volume": km.get("volume", 0),
            })

    return sorted(signals, key=lambda x: x["abs_divergence"], reverse=True)


def compute_divergence_from_pairs(pairs: list) -> list:
    """
    Compute divergences from DB-format pairs (from get_active_pairs_with_prices).
    """
    signals = []
    for p in pairs:
        pm_yes = p.get("pm_yes")
        km_yes = p.get("km_yes")

        if pm_yes is None or km_yes is None:
            continue

        divergence = pm_yes - km_yes
        abs_div = abs(divergence)

        if abs_div >= DIVERGENCE_THRESHOLD:
            signals.append({
                "pair_id": p["id"],
                "polymarket_question": p["pm_question"],
                "kalshi_question": p["km_question"],
                "polymarket_yes": round(pm_yes, 4),
                "kalshi_yes": round(km_yes, 4),
                "divergence": round(divergence, 4),
                "abs_divergence": round(abs_div, 4),
                "higher_on": "polymarket" if divergence > 0 else "kalshi",
                "similarity": p["similarity"],
                "pm_volume": p.get("pm_volume", 0),
                "kalshi_volume": p.get("km_volume", 0),
            })

    return sorted(signals, key=lambda x: x["abs_divergence"], reverse=True)


def _get_pm_yes_price(market: dict) -> float | None:
    """Extract Yes price from Polymarket market data."""
    prices = market.get("outcomePrices")
    if isinstance(prices, str):
        import json
        try:
            prices = json.loads(prices)
        except:
            return None
    if isinstance(prices, list) and len(prices) > 0:
        return float(prices[0])
    tokens = market.get("tokens", [])
    for t in tokens:
        if t.get("outcome") == "Yes":
            return float(t.get("price", 0))
    return None


def _get_kalshi_yes_price(market: dict) -> float | None:
    """Extract Yes price from Kalshi market data."""
    yes_ask = market.get("yes_ask")
    if yes_ask is not None:
        return float(yes_ask) / 100 if float(yes_ask) > 1 else float(yes_ask)
    last = market.get("last_price")
    if last is not None:
        return float(last) / 100 if float(last) > 1 else float(last)
    return None
