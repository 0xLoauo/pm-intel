"""Signal 2: Calibrated probability using Manski bounds.
Signal 3: Favorite-longshot bias adjustment (Snowberg-Wolfers 2010).

Manski (2006): Price is NOT probability. It is a quantile of the
budget-weighted distribution of trader beliefs. The true mean belief
lies in the interval (pi^2, 2*pi - pi^2).

Favorite-longshot bias: Low-probability events are systematically
overpriced, high-probability events are underpriced. Correction:
adjusted = p^gamma / (p^gamma + (1-p)^gamma) where gamma = 0.85.
"""

from config import MANSKI_RISK_ADJUSTMENT, FLB_GAMMA


def manski_bounds(price: float) -> dict:
    """
    Compute Manski probability bounds from a market price.
    Returns lower bound, upper bound, midpoint, and informativeness score.
    """
    if price <= 0 or price >= 1:
        return {"lower": price, "upper": price, "midpoint": price, "informativeness": 1.0}

    # Theoretical bounds: mean belief in (pi^2, 2*pi - pi^2)
    lower = price ** 2
    upper = 2 * price - price ** 2

    # Clamp to [0, 1]
    lower = max(0.0, lower)
    upper = min(1.0, upper)

    midpoint = (lower + upper) / 2
    width = upper - lower

    # Informativeness: how narrow the bounds are (0 = uninformative, 1 = very informative)
    # At price=0.5, width=0.5 (least informative). At price=0.9, width=0.19 (very informative).
    informativeness = 1.0 - (width / 0.5)  # normalized so 0.5 width = 0 informativeness
    informativeness = max(0.0, min(1.0, informativeness))

    # Risk-adjusted point estimate (shrink toward 0.5 by risk adjustment parameter)
    risk_adjusted = price + MANSKI_RISK_ADJUSTMENT * (0.5 - price)
    risk_adjusted = max(0.0, min(1.0, risk_adjusted))

    return {
        "raw_price": round(price, 4),
        "manski_lower": round(lower, 4),
        "manski_upper": round(upper, 4),
        "midpoint": round(midpoint, 4),
        "informativeness": round(informativeness, 3),
        "risk_adjusted": round(risk_adjusted, 4),
    }


def favorite_longshot_adjustment(price: float) -> dict:
    """
    Apply Snowberg-Wolfers (2010) favorite-longshot bias correction.
    Cheap contracts (favorites on the longshot side) tend to be overpriced.
    Expensive contracts tend to be underpriced.
    """
    if price <= 0 or price >= 1:
        return {"raw_price": price, "adjusted_price": price, "bias_correction": 0}

    # Snowberg-Wolfers correction
    p_gamma = price ** FLB_GAMMA
    q_gamma = (1 - price) ** FLB_GAMMA
    adjusted = p_gamma / (p_gamma + q_gamma)

    correction = adjusted - price

    return {
        "raw_price": round(price, 4),
        "adjusted_price": round(adjusted, 4),
        "bias_correction": round(correction, 4),
        "direction": "overpriced" if correction < -0.005 else "underpriced" if correction > 0.005 else "fair",
    }


def calibrate_market(price: float, volume: float = 0, platform: str = "") -> dict:
    """Full calibration: Manski bounds + FLB adjustment + combined assessment."""
    bounds = manski_bounds(price)
    flb = favorite_longshot_adjustment(price)

    # Combined calibrated estimate: average of FLB-adjusted and Manski midpoint
    calibrated_estimate = (flb["adjusted_price"] + bounds["midpoint"]) / 2

    return {
        "raw_price": round(price, 4),
        "calibrated_probability": round(calibrated_estimate, 4),
        "manski_bounds": bounds,
        "flb_adjustment": flb,
        "confidence": bounds["informativeness"],
        "platform": platform,
        "volume": volume,
    }
