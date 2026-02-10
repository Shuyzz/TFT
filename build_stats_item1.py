# build_champ_item1_stats.py
import sqlite3
from collections import defaultdict

DB_PATH = "data/tft.db"

# Basic components we want to EXCLUDE from item performance stats
COMPONENTS = {
    "TFT_Item_BFSword",
    "TFT_Item_ChainVest",
    "TFT_Item_GiantsBelt",
    "TFT_Item_NeedlesslyLargeRod",
    "TFT_Item_NegatronCloak",
    "TFT_Item_RecurveBow",
    "TFT_Item_SparringGloves",
    "TFT_Item_TearOftheGoddess",
    "TFT_Item_Spatula",
}

def is_full_item(item: str) -> bool:
    return bool(item) and (item not in COMPONENTS)

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Single-item "presence" stats table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS champ_item1_stats (
        patch_bucket TEXT,
        champion_id TEXT,
        item_name TEXT,
        n_games INTEGER,
        avg_place REAL,
        top4_rate REAL,
        baseline_avg_place REAL,
        delta REAL,
        PRIMARY KEY (patch_bucket, champion_id, item_name)
    )
    """)
    conn.commit()

    # Baseline: champion appears in a game (per player), regardless of items
    cur.execute("""
    CREATE TEMP VIEW IF NOT EXISTS v_champ_games AS
    SELECT
        m.patch_bucket AS patch_bucket,
        ul.champion_id AS champion_id,
        ul.match_id AS match_id,
        ul.puuid AS puuid
    FROM unit_loadout ul
    JOIN matches m ON m.match_id = ul.match_id
    WHERE m.patch_bucket IS NOT NULL
    GROUP BY m.patch_bucket, ul.champion_id, ul.match_id, ul.puuid
    """)

    cur.execute("""
    CREATE TEMP VIEW IF NOT EXISTS v_champ_baseline AS
    SELECT
        cg.patch_bucket,
        cg.champion_id,
        COUNT(*) AS n_games,
        AVG(pm.placement) AS baseline_avg_place
    FROM v_champ_games cg
    JOIN player_match pm
      ON pm.match_id = cg.match_id AND pm.puuid = cg.puuid
    GROUP BY cg.patch_bucket, cg.champion_id
    """)

    # Pull unit_loadout rows
    cur.execute("""
    SELECT
        m.patch_bucket,
        ul.champion_id,
        ul.match_id,
        ul.puuid,
        ul.items_key,
        pm.placement
    FROM unit_loadout ul
    JOIN matches m ON m.match_id = ul.match_id
    JOIN player_match pm ON pm.match_id = ul.match_id AND pm.puuid = ul.puuid
    WHERE m.patch_bucket IS NOT NULL
    """)
    rows = cur.fetchall()

    # item1 = "presence": count a game once per item even if duplicated
    seen = set()  # (patch, champ, match, puuid, item)
    sum_place = defaultdict(float)
    sum_top4 = defaultdict(float)
    count_games = defaultdict(int)

    for patch_bucket, champ, match_id, puuid, items_key, placement in rows:
        if not items_key:
            continue

        items = [it for it in items_key.split("|") if is_full_item(it)]
        if not items:
            continue

        for item in set(items):
            game_key = (patch_bucket, champ, match_id, puuid, item)
            if game_key in seen:
                continue
            seen.add(game_key)

            k = (patch_bucket, champ, item)
            count_games[k] += 1
            sum_place[k] += float(placement)
            sum_top4[k] += 1.0 if int(placement) <= 4 else 0.0

    # Refresh
    cur.execute("DELETE FROM champ_item1_stats")

    # Insert via temp table then join baseline for delta
    cur.execute("""
    DROP TABLE IF EXISTS tmp_item1
    """)
    cur.execute("""
    CREATE TEMP TABLE tmp_item1 (
        patch_bucket TEXT,
        champion_id TEXT,
        item_name TEXT,
        n_games INTEGER,
        avg_place REAL,
        top4_rate REAL
    )
    """)

    insert_rows = []
    for (patch_bucket, champ, item), n in count_games.items():
        avg_place = sum_place[(patch_bucket, champ, item)] / n
        top4_rate = sum_top4[(patch_bucket, champ, item)] / n
        insert_rows.append((patch_bucket, champ, item, n, avg_place, top4_rate))

    cur.executemany("""
    INSERT INTO tmp_item1(patch_bucket, champion_id, item_name, n_games, avg_place, top4_rate)
    VALUES (?, ?, ?, ?, ?, ?)
    """, insert_rows)

    cur.execute("""
    INSERT INTO champ_item1_stats(
        patch_bucket, champion_id, item_name, n_games, avg_place, top4_rate, baseline_avg_place, delta
    )
    SELECT
        t.patch_bucket,
        t.champion_id,
        t.item_name,
        t.n_games,
        t.avg_place,
        t.top4_rate,
        b.baseline_avg_place,
        (t.avg_place - b.baseline_avg_place) AS delta
    FROM tmp_item1 t
    JOIN v_champ_baseline b
      ON b.patch_bucket = t.patch_bucket AND b.champion_id = t.champion_id
    """)
    conn.commit()

    # Sanity print
    cur.execute("SELECT DISTINCT patch_bucket FROM matches WHERE patch_bucket IS NOT NULL ORDER BY patch_bucket")
    buckets = [r[0] for r in cur.fetchall()]
    print("Patch buckets:", buckets)

    if buckets:
        target_bucket = buckets[-1]
        champ = "TFT16_Wukong"
        print(f"\nTop single items by BEST delta for {champ} in {target_bucket} (min 5 games):")
        cur.execute("""
        SELECT item_name, n_games, avg_place, baseline_avg_place, delta, top4_rate
        FROM champ_item1_stats
        WHERE patch_bucket = ? AND champion_id = ? AND n_games >= 5
        ORDER BY delta ASC
        LIMIT 15
        """, (target_bucket, champ))
        for item_name, n, avgp, basep, d, t4 in cur.fetchall():
            print(f"{item_name:40} n={n:4d} avg={avgp:.2f} base={basep:.2f} delta={d:+.2f} top4={t4:.2f}")

    # Extra sanity: ensure no components leaked
    cur.execute("""
    SELECT COUNT(*)
    FROM champ_item1_stats
    WHERE item_name IN (
        'TFT_Item_BFSword','TFT_Item_ChainVest','TFT_Item_GiantsBelt','TFT_Item_NeedlesslyLargeRod',
        'TFT_Item_NegatronCloak','TFT_Item_RecurveBow','TFT_Item_SparringGloves','TFT_Item_TearoftheGoddess',
        'TFT_Item_Spatula'
    )
    """)
    leaked = cur.fetchone()[0]
    print(f"\nComponent leakage rows in champ_item1_stats: {leaked}")

    conn.close()
    print("\nDone: built champ_item1_stats")

if __name__ == "__main__":
    main()
