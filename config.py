"""Configuration for PM Intelligence Feed."""

# API endpoints
POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com"
POLYMARKET_CLOB_API = "https://clob.polymarket.com"
KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"

# Data
DB_PATH = "/root/pm-intel/data/pm_intel.db"

# Collection
COLLECT_INTERVAL_SECONDS = 60
KALSHI_PAGE_SIZE = 200

# Signal thresholds
DIVERGENCE_THRESHOLD = 0.05      # 5 cent minimum divergence to flag
THIN_MARKET_VOLUME = 10000       # 0K minimum volume
ILLIQUID_SPREAD = 0.10           # 10 cent bid-ask spread = illiquid
MATCH_SIMILARITY_THRESHOLD = 0.55  # Fuzzy match threshold

# Manski risk-aversion parameter (moderate)
MANSKI_RISK_ADJUSTMENT = 0.15

# Favorite-longshot bias correction exponent (Snowberg-Wolfers 2010)
FLB_GAMMA = 0.85

# API server
API_HOST = "0.0.0.0"
API_PORT = 8420
