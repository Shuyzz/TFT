import os
import json
import sqlite3
from glob import glob

DB_PATH = "data/tft.db"
RAW_DIR = "data/raw_matches"

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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS unit_item (
        match_id TEXT,
        puuid TEXT,
        champion_id TEXT,
        unit_tier INTEGER,
        item_name TEXT
    )
    """)

    # Helpful indexes for speed later
    cur.execute("CREATE INDEX IF NOT EXISTS idx_unit_item_champ ON unit_item(champion_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_unit_item_item  ON unit_item(item_name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_player_match_place ON player_match(placement)")

    conn.commit()

def ingest_one_match(conn: sqlite3.Connection, match: dict):
    cur = conn.cursor()

    match_id = match["metadata"]["match_id"]
    info = match["info"]
    game_datetime = info.get("game_datetime")
    game_version = info.get("game_version")
    tft_set_number = info.get("tft_set_number")

    # patch bucket: from derived if present, else NULL
    patch_bucket = None
    if "_derived" in match and "patch_bucket" in match["_derived"]:
        patch_bucket = match["_derived"]["patch_bucket"]

    # Insert match row
    cur.execute("""
    INSERT OR IGNORE INTO matches(match_id, game_datetime, patch_bucket, game_version, tft_set_number)
    VALUES (?, ?, ?, ?, ?)
    """, (match_id, game_datetime, patch_bucket, game_version, tft_set_number))

    # Insert participants + unit items
    for p in info["participants"]:
        puuid = p["puuid"]
        placement = p.get("placement")
        riot_name = p.get("riotIdGameName")
        riot_tag = p.get("riotIdTagline")

        cur.execute("""
        INSERT OR IGNORE INTO player_match(match_id, puuid, placement, riot_id_game_name, riot_id_tag_line)
        VALUES (?, ?, ?, ?, ?)
        """, (match_id, puuid, placement, riot_name, riot_tag))

        # Units and their items
        for u in p.get("units", []):
            champ = u.get("character_id")
            tier = u.get("tier")
            items = u.get("itemNames", []) or []
            for item_name in items:
                cur.execute("""
                INSERT INTO unit_item(match_id, puuid, champion_id, unit_tier, item_name)
                VALUES (?, ?, ?, ?, ?)
                """, (match_id, puuid, champ, tier, item_name))

    conn.commit()

def main():
    os.makedirs("data", exist_ok=True)

    json_files = sorted(glob(os.path.join(RAW_DIR, "*.json")))
    if not json_files:
        raise RuntimeError(f"No JSON files found in {RAW_DIR}")

    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)

    ingested = 0
    for fp in json_files:
        with open(fp, "r", encoding="utf-8") as f:
            match = json.load(f)
        ingest_one_match(conn, match)
        ingested += 1

    print(f"Built DB: {DB_PATH}")
    print(f"Ingested matches: {ingested}")

    # Quick sanity checks:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM matches")
    print("matches rows:", cur.fetchone()[0])

    cur.execute("SELECT COUNT(*) FROM player_match")
    print("player_match rows:", cur.fetchone()[0])

    cur.execute("SELECT COUNT(*) FROM unit_item")
    print("unit_item rows:", cur.fetchone()[0])

    conn.close()

if __name__ == "__main__":
    main()
