"""Configuration for PM Intelligence Feed v3.

Architecture:
- Discovery scan (3x daily): full market fetch + matching
- Price monitor (every 2 min): prices for known pairs only
- API server: serves cached data
"""

# API endpoints
POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com"
POLYMARKET_CLOB_API = "https://clob.polymarket.com"
KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"

# Data
DB_PATH = "/root/pm-intel/data/pm_intel.db"

# --- Discovery scan schedule (UTC hours) ---
# 06:00 UTC — catches overnight new markets
# 14:00 UTC (9am EST) — US open, most new markets drop
# 22:00 UTC (5pm EST) — end of US day
DISCOVERY_HOURS_UTC = [6, 14, 22]

# --- Price monitor ---
PRICE_MONITOR_INTERVAL = 120  # seconds between price checks on known pairs

# --- Matching ---
MATCH_SIMILARITY_THRESHOLD = 0.55

# --- Signal thresholds ---
DIVERGENCE_THRESHOLD = 0.05      # 5 cent minimum divergence to flag
THIN_MARKET_VOLUME = 10000       # 10K minimum volume
ILLIQUID_SPREAD = 0.10           # 10 cent bid-ask spread = illiquid

# Manski risk-aversion parameter (moderate)
MANSKI_RISK_ADJUSTMENT = 0.15

# Favorite-longshot bias correction exponent (Snowberg-Wolfers 2010)
FLB_GAMMA = 0.85

# API server
API_HOST = "0.0.0.0"
API_PORT = 8420

# DB retention
SNAPSHOT_RETENTION_DAYS = 30
