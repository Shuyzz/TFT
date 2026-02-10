RAW_DIR = "data/raw_matches_10k"
DB_PATH = "data/tft_10k.db"
SEEN_PATH = "data/match_id_seen_10k.json"
STATE_PATH = "data/fetch_state_10k.json"

TARGET_MATCHES = 10_000

# Riot routing:
PLATFORM = "na1"                 # ladder endpoint host (platform routing)
REGION_HOST = "https://americas.api.riotgames.com"  # match-v1 host (regional routing)
PLATFORM_HOST = f"https://{PLATFORM}.api.riotgames.com"
