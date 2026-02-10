import os
import json
import time
import random
import requests
from dotenv import load_dotenv

from config_10k import RAW_DIR, SEEN_PATH, STATE_PATH, TARGET_MATCHES, PLATFORM_HOST, REGION_HOST

load_dotenv()
API_KEY = os.getenv("RIOT_API_KEY")
if not API_KEY:
    raise RuntimeError("Missing RIOT_API_KEY in .env")

HEADERS = {"X-Riot-Token": API_KEY}

# Conservative rate limits (safe for overnight):
# - Match details are the heavy part. Keep steady pace.
REQ_SLEEP_SECONDS = 1.4  # ~43 req/min; safely under 100 per 2 minutes

# TFT ranked queues: most ranked TFT matches are queue_id 1100 (Ranked)
RANKED_QUEUE_ID = 1100

def get_json(url, params=None):
    while True:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)

        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "5"))
            print(f"[429] Rate limited. Sleeping {wait}s...")
            time.sleep(wait + 1)
            continue

        if r.status_code >= 500:
            print(f"[{r.status_code}] Server error. Sleeping 10s...")
            time.sleep(10)
            continue

        if r.status_code >= 400:
            print(f"[{r.status_code}] {r.text[:300]}")
            r.raise_for_status()

        return r.json()

def load_json(path, default):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default

def save_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def ensure_dirs():
    os.makedirs("data", exist_ok=True)
    os.makedirs(RAW_DIR, exist_ok=True)

def already_downloaded(match_id: str) -> bool:
    return os.path.exists(os.path.join(RAW_DIR, f"{match_id}.json"))

def fetch_ladder_puuids():
    """
    Get a seed set of PUUIDs by crawling Master/GM/Challenger leagues on the platform host.
    Endpoints:
      /tft/league/v1/challenger
      /tft/league/v1/grandmaster
      /tft/league/v1/master
    """
    tiers = ["challenger", "grandmaster", "master"]
    puuids = set()

    for tier in tiers:
        url = f"{PLATFORM_HOST}/tft/league/v1/{tier}"
        data = get_json(url)
        entries = data.get("entries", [])
        print(f"Loaded {len(entries)} entries from {tier}")

        # entries contain summonerId, not PUUID -> we need summoner-v4 lookup
        for e in entries:
            sid = e.get("summonerId")
            if not sid:
                continue
            s = get_json(f"{PLATFORM_HOST}/tft/summoner/v1/summoners/{sid}")
            puuid = s.get("puuid")
            if puuid:
                puuids.add(puuid)

        time.sleep(1.0)  # small pause between tiers

    puuids = list(puuids)
    random.shuffle(puuids)
    print(f"Seed PUUIDs total: {len(puuids)}")
    return puuids

def fetch_match_ids_for_puuid(puuid: str, start=0, count=200):
    """
    Pull a page of match IDs for a given PUUID.
    """
    url = f"{REGION_HOST}/tft/match/v1/matches/by-puuid/{puuid}/ids"
    params = {
        "start": start,
        "count": count,
        "queue": RANKED_QUEUE_ID,  # keep ranked only
    }
    return get_json(url, params=params)

def fetch_match_detail(match_id: str):
    url = f"{REGION_HOST}/tft/match/v1/matches/{match_id}"
    return get_json(url)

def main():
    ensure_dirs()

    seen = set(load_json(SEEN_PATH, []))
    state = load_json(STATE_PATH, {
        "puuids": [],
        "puuid_idx": 0,
        "page_start": 0
    })

    if not state["puuids"]:
        print("No saved state: fetching seed PUUID list from Master/GM/Challenger...")
        state["puuids"] = fetch_ladder_puuids()
        state["puuid_idx"] = 0
        state["page_start"] = 0
        save_json(STATE_PATH, state)

    puuids = state["puuids"]
    puuid_idx = state["puuid_idx"]
    page_start = state["page_start"]

    print(f"Resume: seen={len(seen)} target={TARGET_MATCHES} puuids={len(puuids)} idx={puuid_idx} start={page_start}")

    while len(seen) < TARGET_MATCHES:
        if puuid_idx >= len(puuids):
            print("Ran out of seed PUUIDs. Refreshing ladder list...")
            puuids = fetch_ladder_puuids()
            state["puuids"] = puuids
            puuid_idx = 0
            page_start = 0

        puuid = puuids[puuid_idx]

        # pull match ids in pages
        try:
            match_ids = fetch_match_ids_for_puuid(puuid, start=page_start, count=200)
        except Exception as e:
            print(f"Error fetching match ids for puuid_idx={puuid_idx}: {e}")
            time.sleep(10)
            puuid_idx += 1
            page_start = 0
            continue

        if not match_ids:
            # no more matches for this player
            puuid_idx += 1
            page_start = 0
            state["puuid_idx"] = puuid_idx
            state["page_start"] = page_start
            save_json(STATE_PATH, state)
            continue

        # Process match ids
        for mid in match_ids:
            if len(seen) >= TARGET_MATCHES:
                break
            if mid in seen and already_downloaded(mid):
                continue

            # Fetch match detail and save
            try:
                match = fetch_match_detail(mid)
            except Exception as e:
                print(f"Error fetching match {mid}: {e}")
                time.sleep(10)
                continue

            # Save JSON
            outpath = os.path.join(RAW_DIR, f"{mid}.json")
            with open(outpath, "w", encoding="utf-8") as f:
                json.dump(match, f, ensure_ascii=False, indent=2)

            seen.add(mid)

            # Persist progress every match (safer for overnight)
            save_json(SEEN_PATH, sorted(list(seen)))
            state["puuid_idx"] = puuid_idx
            state["page_start"] = page_start
            save_json(STATE_PATH, state)

            if len(seen) % 50 == 0:
                print(f"Progress: {len(seen)}/{TARGET_MATCHES} saved. Latest={mid}")

            time.sleep(REQ_SLEEP_SECONDS)

        # next page for same player
        page_start += 200

        # if we fetched a lot for this player, rotate to next to diversify
        if page_start >= 600:  # 3 pages per player, then move on
            puuid_idx += 1
            page_start = 0

        state["puuid_idx"] = puuid_idx
        state["page_start"] = page_start
        save_json(STATE_PATH, state)

    print(f"Done! Collected {len(seen)} matches in {RAW_DIR}")

if __name__ == "__main__":
    main()
