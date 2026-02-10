import os
import json
import sqlite3
from glob import glob

DB_PATH = "data/tft_10k.db"
RAW_DIR = "data/raw_matches_10k"

def create_tables(conn: sqlite3.Connection):
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS matches (
        match_id TEXT PRIMARY KEY,
        game_datetime INTEGER,
        patch_bucket TEXT,
        game_version TEXT,
        tft_set_number INTEGER
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS player_match (
        match_id TEXT,
        puuid TEXT,
        placement INTEGER,
        riot_id_game_name TEXT,
        riot_id_tag_line TEXT,
        PRIMARY KEY (match_id, puuid)
    )
    """)

    # NOTE: this assumes you already have unit_loadout in your newer DB design.
    # If your ingestion still uses unit_item, tell me and I’ll adjust.
    cur.execute("""
    CREATE TABLE IF NOT EXISTS unit_loadout (
        match_id TEXT,
        puuid TEXT,
        champion_id TEXT,
        unit_tier INTEGER,
        items_key TEXT
    )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_ul_champ ON unit_loadout(champion_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pm_place ON player_match(placement)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_m_patch ON matches(patch_bucket)")

    conn.commit()

def infer_patch_bucket(match: dict) -> str | None:
    """
    If you already store match['_derived']['patch_bucket'], we use it.
    Else fallback to using info['game_version'] like 'Version 16.4.x...' -> 'TFT16.4'
    """
    if "_derived" in match and "patch_bucket" in match["_derived"]:
        return match["_derived"]["patch_bucket"]

    gv = match.get("info", {}).get("game_version")
    if not gv:
        return None

    # try extract major.minor: e.g. "Version 16.4.560.1234" or "16.4.560..."
    parts = gv.replace("Version", "").strip().split(".")
    if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
        return f"TFT{parts[0]}.{parts[1]}"
    return None

def ingest_one_match(conn: sqlite3.Connection, match: dict):
    cur = conn.cursor()

    match_id = match["metadata"]["match_id"]
    info = match["info"]
    game_datetime = info.get("game_datetime")
    game_version = info.get("game_version")
    tft_set_number = info.get("tft_set_number")
    patch_bucket = infer_patch_bucket(match)

    cur.execute("""
    INSERT OR IGNORE INTO matches(match_id, game_datetime, patch_bucket, game_version, tft_set_number)
    VALUES (?, ?, ?, ?, ?)
    """, (match_id, game_datetime, patch_bucket, game_version, tft_set_number))

    for p in info["participants"]:
        puuid = p["puuid"]
        placement = p.get("placement")
        riot_name = p.get("riotIdGameName")
        riot_tag = p.get("riotIdTagline")

        cur.execute("""
        INSERT OR IGNORE INTO player_match(match_id, puuid, placement, riot_id_game_name, riot_id_tag_line)
        VALUES (?, ?, ?, ?, ?)
        """, (match_id, puuid, placement, riot_name, riot_tag))

        for u in p.get("units", []):
            champ = u.get("character_id")
            tier = u.get("tier")
            items = u.get("itemNames", []) or []
            items_key = "|".join(items) if items else None

            cur.execute("""
            INSERT INTO unit_loadout(match_id, puuid, champion_id, unit_tier, items_key)
            VALUES (?, ?, ?, ?, ?)
            """, (match_id, puuid, champ, tier, items_key))

    conn.commit()

def main():
    os.makedirs("data", exist_ok=True)

    json_files = sorted(glob(os.path.join(RAW_DIR, "*.json")))
    if not json_files:
        raise RuntimeError(f"No JSON files found in {RAW_DIR}")

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)  # rebuild clean for 10k

    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)

    ingested = 0
    for fp in json_files:
        with open(fp, "r", encoding="utf-8") as f:
            match = json.load(f)
        ingest_one_match(conn, match)
        ingested += 1
        if ingested % 200 == 0:
            print(f"Ingested {ingested}/{len(json_files)}...")

    print(f"Built DB: {DB_PATH}")
    print(f"Ingested matches: {ingested}")

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM matches")
    print("matches rows:", cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM player_match")
    print("player_match rows:", cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM unit_loadout")
    print("unit_loadout rows:", cur.fetchone()[0])

    conn.close()

if __name__ == "__main__":
    main()
