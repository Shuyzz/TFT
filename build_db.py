import os
import json
import sqlite3
from glob import glob

DB_PATH = "data/tft.db"
RAW_DIR = "data/raw_matches"

# Set True if you want to rebuild from scratch each time
WIPE_DB = False

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

    # Item-level rows (kept for simple counts / joins)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS unit_item (
        match_id TEXT,
        puuid TEXT,
        champion_id TEXT,
        unit_tier INTEGER,
        item_name TEXT
    )
    """)

    # NEW: One row per unit appearance, with the full item loadout encoded.
    # items_key is sorted and preserves duplicates:
    #   ""  (no items)
    #   "TFT_Item_GargoyleStoneplate"
    #   "TFT_Item_GargoyleStoneplate|TFT_Item_WarmogsArmor"
    #   "TFT_Item_GargoyleStoneplate|TFT_Item_GargoyleStoneplate|TFT_Item_WarmogsArmor"
    cur.execute("""
    CREATE TABLE IF NOT EXISTS unit_loadout (
        match_id TEXT,
        puuid TEXT,
        champion_id TEXT,
        unit_tier INTEGER,
        items_key TEXT,
        PRIMARY KEY (match_id, puuid, champion_id, unit_tier, items_key)
    )
    """)

    # Helpful indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_unit_item_champ ON unit_item(champion_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_unit_item_item  ON unit_item(item_name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_player_match_place ON player_match(placement)")

    cur.execute("CREATE INDEX IF NOT EXISTS idx_loadout_champ ON unit_loadout(champion_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_loadout_items ON unit_loadout(items_key)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_matches_patch ON matches(patch_bucket)")

    conn.commit()

def wipe_tables(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("DELETE FROM unit_item")
    cur.execute("DELETE FROM unit_loadout")
    cur.execute("DELETE FROM player_match")
    cur.execute("DELETE FROM matches")
    conn.commit()

def make_items_key(items):
    """Return canonical items_key string. Sorted, duplicates preserved."""
    if not items:
        return ""
    items_sorted = sorted(items)
    return "|".join(items_sorted)

def ingest_one_match(conn: sqlite3.Connection, match: dict):
    cur = conn.cursor()

    match_id = match["metadata"]["match_id"]
    info = match["info"]
    game_datetime = info.get("game_datetime")
    game_version = info.get("game_version")
    tft_set_number = info.get("tft_set_number")

    patch_bucket = None
    if "_derived" in match and isinstance(match["_derived"], dict):
        patch_bucket = match["_derived"].get("patch_bucket")

    # Insert match row
    cur.execute("""
    INSERT OR IGNORE INTO matches(match_id, game_datetime, patch_bucket, game_version, tft_set_number)
    VALUES (?, ?, ?, ?, ?)
    """, (match_id, game_datetime, patch_bucket, game_version, tft_set_number))

    # Insert participants + units
    for p in info.get("participants", []):
        puuid = p["puuid"]
        placement = p.get("placement")
        riot_name = p.get("riotIdGameName")
        riot_tag = p.get("riotIdTagline")

        cur.execute("""
        INSERT OR IGNORE INTO player_match(match_id, puuid, placement, riot_id_game_name, riot_id_tag_line)
        VALUES (?, ?, ?, ?, ?)
        """, (match_id, puuid, placement, riot_name, riot_tag))

        for u in p.get("units", []) or []:
            champ = u.get("character_id")
            tier = u.get("tier")

            items = u.get("itemNames", []) or []
            # Always write the loadout row (even if items == [])
            items_key = make_items_key(items)

            cur.execute("""
            INSERT OR IGNORE INTO unit_loadout(match_id, puuid, champion_id, unit_tier, items_key)
            VALUES (?, ?, ?, ?, ?)
            """, (match_id, puuid, champ, tier, items_key))

            # Also write item-level rows (one row per item)
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

    if WIPE_DB:
        wipe_tables(conn)

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

    cur.execute("SELECT COUNT(*) FROM unit_loadout")
    print("unit_loadout rows:", cur.fetchone()[0])

    cur.execute("SELECT COUNT(*) FROM unit_loadout WHERE items_key = ''")
    print("unit_loadout rows with NO items (items_key=''):", cur.fetchone()[0])

    # Optional peek: show a few loadouts
    cur.execute("""
    SELECT champion_id, items_key, COUNT(*) as n
    FROM unit_loadout
    GROUP BY champion_id, items_key
    ORDER BY n DESC
    LIMIT 10
    """)
    print("\nTop loadouts (champion_id, items_key, count):")
    for r in cur.fetchall():
        print(r)

    conn.close()

if __name__ == "__main__":
    main()
