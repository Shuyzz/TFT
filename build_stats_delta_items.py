import sqlite3

DB_PATH = "data/tft.db"

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Create stats table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS champ_item_stats (
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

    # 1) Build baseline per (patch_bucket, champion)
    # Baseline is computed from unique (match_id, puuid, champion_id) rows.
    cur.execute("""
    CREATE TEMP VIEW IF NOT EXISTS v_champ_games AS
    SELECT
        m.patch_bucket AS patch_bucket,
        ui.champion_id AS champion_id,
        ui.match_id AS match_id,
        ui.puuid AS puuid
    FROM unit_item ui
    JOIN matches m ON m.match_id = ui.match_id
    GROUP BY m.patch_bucket, ui.champion_id, ui.match_id, ui.puuid
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

    # 2) Compute item stats per (patch_bucket, champion, item)
    # Count unique games for that champion-item pair (avoid double counting)
    cur.execute("""
    CREATE TEMP VIEW IF NOT EXISTS v_champ_item_games AS
    SELECT
        m.patch_bucket AS patch_bucket,
        ui.champion_id AS champion_id,
        ui.item_name AS item_name,
        ui.match_id AS match_id,
        ui.puuid AS puuid
    FROM unit_item ui
    JOIN matches m ON m.match_id = ui.match_id
    GROUP BY m.patch_bucket, ui.champion_id, ui.item_name, ui.match_id, ui.puuid
    """)

    # Fill/refresh stats table
    cur.execute("DELETE FROM champ_item_stats")

    cur.execute("""
    INSERT INTO champ_item_stats (
        patch_bucket, champion_id, item_name, n_games, avg_place, top4_rate, baseline_avg_place, delta
    )
    SELECT
        cig.patch_bucket,
        cig.champion_id,
        cig.item_name,
        COUNT(*) AS n_games,
        AVG(pm.placement) AS avg_place,
        AVG(CASE WHEN pm.placement <= 4 THEN 1.0 ELSE 0.0 END) AS top4_rate,
        cb.baseline_avg_place AS baseline_avg_place,
        (AVG(pm.placement) - cb.baseline_avg_place) AS delta
    FROM v_champ_item_games cig
    JOIN player_match pm
      ON pm.match_id = cig.match_id AND pm.puuid = cig.puuid
    JOIN v_champ_baseline cb
      ON cb.patch_bucket = cig.patch_bucket AND cb.champion_id = cig.champion_id
    GROUP BY cig.patch_bucket, cig.champion_id, cig.item_name
    """)
    conn.commit()

    # Sanity print: top 15 items by delta for Wukong in latest patch bucket found
    cur.execute("SELECT DISTINCT patch_bucket FROM matches ORDER BY patch_bucket")
    buckets = [r[0] for r in cur.fetchall()]
    print("Patch buckets in DB:", buckets)

    target_bucket = buckets[-1] if buckets else None
    champ = "TFT16_Wukong"

    if target_bucket:
        print(f"\nTop items by BEST delta for {champ} in {target_bucket} (min 5 games):")
        cur.execute("""
        SELECT item_name, n_games, avg_place, baseline_avg_place, delta, top4_rate
        FROM champ_item_stats
        WHERE patch_bucket = ? AND champion_id = ? AND n_games >= 5
        ORDER BY delta ASC
        LIMIT 15
        """, (target_bucket, champ))
        rows = cur.fetchall()
        for item_name, n, avgp, basep, d, t4 in rows:
            print(f"{item_name:40} n={n:4d} avg={avgp:.2f} base={basep:.2f} delta={d:+.2f} top4={t4:.2f}")

    conn.close()
    print("\nDone: built champ_item_stats")

if __name__ == "__main__":
    main()

#testys 