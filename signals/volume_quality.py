"""Signal 8: Volume quality filter.
Detects thin markets, wash trading indicators, and assigns quality scores.
Based on Irkham research (~25% wash trading on some platforms).
"""

from config import THIN_MARKET_VOLUME, ILLIQUID_SPREAD


def assess_quality(market: dict, platform: str) -> dict:
    """
    Assess the signal quality of a market based on volume, spread, and depth.
    Returns a quality score 0-100 and flags.
    """
    flags = []
    score = 100

    volume = float(market.get("volume", 0) or 0)
    
    # Volume check
    if volume < THIN_MARKET_VOLUME:
        flags.append("thin_market")
        score -= 40
    elif volume < THIN_MARKET_VOLUME * 5:
        score -= 10  # Low but not critical
    
    # Spread check (if available)
    if platform == "polymarket":
        tokens = market.get("tokens", [])
        if len(tokens) >= 2:
            prices = [float(t.get("price", 0)) for t in tokens]
            if len(prices) >= 2:
                implied_spread = abs(1.0 - sum(prices))
                if implied_spread > ILLIQUID_SPREAD:
                    flags.append("illiquid")
                    score -= 20
    elif platform == "kalshi":
        yes_ask = market.get("yes_ask")
        yes_bid = market.get("yes_bid")
        if yes_ask is not None and yes_bid is not None:
            spread = float(yes_ask) - float(yes_bid)
            if spread > ILLIQUID_SPREAD * 100:  # Kalshi uses cents
                flags.append("illiquid")
                score -= 20

    # Open interest check (Kalshi)
    if platform == "kalshi":
        oi = float(market.get("open_interest", 0) or 0)
        if oi < 100:
            flags.append("low_open_interest")
            score -= 15

    # Activity check — recently traded?
    if volume == 0:
        flags.append("no_activity")
        score -= 50

    score = max(0, min(100, score))

    quality_label = "high" if score >= 70 else "medium" if score >= 40 else "low"

    return {
        "quality_score": score,
        "quality_label": quality_label,
        "volume": volume,
        "flags": flags,
        "platform": platform,
    }
