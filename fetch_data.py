import os
import json
import time
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv

# 1) Load API key from .env (safe)
load_dotenv()
API_KEY = os.getenv("RIOT_API_KEY")
if not API_KEY:
    raise RuntimeError("Missing RIOT_API_KEY in .env")

HEADERS = {"X-Riot-Token": API_KEY}

REGION_HOST = "https://americas.api.riotgames.com"

def get_json(url, params=None):
    """GET JSON with basic rate-limit retry."""
    while True:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)

        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "2"))
            print(f"Rate limited. Waiting {wait}s...")
            time.sleep(wait)
            continue

        if r.status_code >= 400:
            print("Error status:", r.status_code)
            print("Response:", r.text[:300])
            r.raise_for_status()

        return r.json()

# --- Patch bucketing (date-based) ---
PATCH_WINDOWS = [
    ("TFT16.2", datetime(2026, 1, 8, tzinfo=timezone.utc)),
    ("TFT16.3", datetime(2026, 1, 22, tzinfo=timezone.utc)),
    ("TFT16.4", datetime(2026, 2, 4, tzinfo=timezone.utc)),
]

def patch_bucket(game_datetime_ms: int) -> str:
    dt = datetime.fromtimestamp(game_datetime_ms / 1000, tz=timezone.utc)
    bucket = "TFT16.1x"
    for name, start_dt in PATCH_WINDOWS:
        if dt >= start_dt:
            bucket = name
    return bucket

def ms_to_utc_str(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

# ====== EDIT THESE ======
GAME_NAME = "VIT k3soju"
TAG_LINE  = "000"
COUNT = 30  # start with 30; later you can do 200+
# =======================

def main():
    # 1) Riot ID -> PUUID
    acct = get_json(f"{REGION_HOST}/riot/account/v1/accounts/by-riot-id/{GAME_NAME}/{TAG_LINE}")
    puuid = acct["puuid"]
    print("PUUID:", puuid)

    # 2) PUUID -> match IDs
    match_ids = get_json(
        f"{REGION_HOST}/tft/match/v1/matches/by-puuid/{puuid}/ids",
        params={"start": 0, "count": COUNT}
    )
    print(f"Got {len(match_ids)} match IDs")

    # 3) Fetch and cache matches
    os.makedirs("data/raw_matches", exist_ok=True)

    saved = 0
    skipped = 0

    for mid in match_ids:
        outpath = f"data/raw_matches/{mid}.json"

        # Skip if already downloaded
        if os.path.exists(outpath):
            skipped += 1
            continue

        match = get_json(f"{REGION_HOST}/tft/match/v1/matches/{mid}")

        # Add a tiny derived field (optional but useful)
        gms = match["info"]["game_datetime"]
        bucket = patch_bucket(gms)

        # Save the raw match as-is + a small header field for convenience
        match["_derived"] = {
            "patch_bucket": bucket,
            "game_datetime_utc": ms_to_utc_str(gms)
        }

        with open(outpath, "w", encoding="utf-8") as f:
            json.dump(match, f, ensure_ascii=False, indent=2)

        saved += 1
        print(f"Saved {mid} | {match['_derived']['game_datetime_utc']} | {bucket}")

        # Gentle pacing to avoid hammering
        time.sleep(0.15)

    print(f"Done. Saved={saved}, Skipped(existing)={skipped}")

if __name__ == "__main__":
    main()
