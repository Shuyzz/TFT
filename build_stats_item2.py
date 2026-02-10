# build_champ_item2_stats.py
import sqlite3
from collections import defaultdict

DB_PATH = "data/tft.db"

# IMPORTANT: component names must match Riot's exact strings.
BASIC_COMPONENTS = {
    "TFT_Item_BFSword",
    "TFT_Item_ChainVest",
    "TFT_Item_GiantsBelt",
    "TFT_Item_NeedlesslyLargeRod",
    "TFT_Item_NegatronCloak",
    "TFT_Item_RecurveBow",
    "TFT_Item_SparringGloves",
    "TFT_Item_TearOfTheGoddess",  # <-- FIXED (was TearoftheGoddess)
    "TFT_Item_Spatula",
}

def is_full_item(item: str) -> bool:
    return bool(item) and (item not in BASIC_COMPONENTS)

def canon_item2(a: str, b: str) -> tuple[str, str]:
    # Keep duplicates: (x,x) stays (x,x). Sort for canonical order.
    return tuple(sorted((a, b)))

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS champ_item2_stats (
        patch_bucket TEXT,
        champion_id TEXT,
        item1 TEXT,
        item2 TEXT,
        n_games INTEGER,
        avg_place REAL,
        top4_rate REAL,
        baseline_avg_place REAL,
        delta REAL,
        PRIMARY KEY (patch_bucket, champion_id, item1, item2)
    )
    """)
    conn.commit()

    # Baseline per champ per patch
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

    # Pull rows
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

    # item2 = any 2-item combination from the unit's FULL items
    # If a unit has 3 full items, it contributes all 2-combinations.
    seen = set()  # (patch, champ, match, puuid, item1, item2)
    sum_place = defaultdict(float)
    sum_top4 = defaultdict(float)
    count_games = defaultdict(int)

    for patch_bucket, champ, match_id, puuid, items_key, placement in rows:
        if placement is None:
            continue
        if not items_key:
            continue

        raw_items = [it.strip() for it in items_key.split("|") if it and it.strip()]
        items = [it for it in raw_items if is_full_item(it)]

        if len(items) < 2:
            continue

        placement_f = float(placement)
        is_top4 = 1.0 if int(placement) <= 4 else 0.0

        # generate 2-combinations, respecting duplicates
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                a, b = canon_item2(items[i], items[j])

                game_key = (patch_bucket, champ, match_id, puuid, a, b)
                if game_key in seen:
                    continue
                seen.add(game_key)

                k = (patch_bucket, champ, a, b)
                count_games[k] += 1
                sum_place[k] += placement_f
                sum_top4[k] += is_top4

    # Refresh
    cur.execute("DELETE FROM champ_item2_stats")

    cur.execute("DROP TABLE IF EXISTS tmp_item2")
    cur.execute("""
    CREATE TEMP TABLE tmp_item2 (
        patch_bucket TEXT,
        champion_id TEXT,
        item1 TEXT,
        item2 TEXT,
        n_games INTEGER,
        avg_place REAL,
        top4_rate REAL
    )
    """)

    insert_rows = []
    for (patch_bucket, champ, a, b), n in count_games.items():
        avg_place = sum_place[(patch_bucket, champ, a, b)] / n
        top4_rate = sum_top4[(patch_bucket, champ, a, b)] / n
        insert_rows.append((patch_bucket, champ, a, b, n, avg_place, top4_rate))

    cur.executemany("""
    INSERT INTO tmp_item2(patch_bucket, champion_id, item1, item2, n_games, avg_place, top4_rate)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, insert_rows)

    cur.execute("""
    INSERT INTO champ_item2_stats(
        patch_bucket, champion_id, item1, item2, n_games, avg_place, top4_rate, baseline_avg_place, delta
    )
    SELECT
        t.patch_bucket,
        t.champion_id,
        t.item1,
        t.item2,
        t.n_games,
        t.avg_place,
        t.top4_rate,
        b.baseline_avg_place,
        (t.avg_place - b.baseline_avg_place) AS delta
    FROM tmp_item2 t
    JOIN v_champ_baseline b
      ON b.patch_bucket = t.patch_bucket AND b.champion_id = t.champion_id
    """)
    conn.commit()

    # Quick sanity: prove components are not present
    cur.execute("""
    SELECT COUNT(*)
    FROM champ_item2_stats
    WHERE item1 IN ({}) OR item2 IN ({})
    """.format(
        ",".join(["?"] * len(BASIC_COMPONENTS)),
        ",".join(["?"] * len(BASIC_COMPONENTS)),
    ), tuple(BASIC_COMPONENTS) + tuple(BASIC_COMPONENTS))
    bad = cur.fetchone()[0]
    print("Component rows in champ_item2_stats (should be 0):", bad)

    # Show something
    cur.execute("SELECT DISTINCT patch_bucket FROM matches WHERE patch_bucket IS NOT NULL ORDER BY patch_bucket")
    buckets = [r[0] for r in cur.fetchall()]
    print("Patch buckets:", buckets)
    if buckets:
        target_bucket = buckets[-1]
        champ = "TFT16_Wukong"
        print(f"\nTop 2-item combos by BEST delta for {champ} in {target_bucket} (min 5 games):")
        cur.execute("""
        SELECT item1, item2, n_games, avg_place, baseline_avg_place, delta, top4_rate
        FROM champ_item2_stats
        WHERE patch_bucket = ? AND champion_id = ? AND n_games >= 5
        ORDER BY delta ASC
        LIMIT 15
        """, (target_bucket, champ))
        for a, b, n, avgp, basep, d, t4 in cur.fetchall():
            print(f"{a} + {b}   n={n:4d} avg={avgp:.2f} base={basep:.2f} delta={d:+.2f} top4={t4:.2f}")

    conn.close()
    print("\nDone: built champ_item2_stats")

if __name__ == "__main__":
    main()
