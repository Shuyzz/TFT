import sqlite3, pandas as pd
from pathlib import Path

Path("data/exports").mkdir(parents=True, exist_ok=True)

con = sqlite3.connect("data/tft.db")
df = pd.read_sql("SELECT * FROM champ_item1_stats", con)
df.to_csv("data/exports/champ_item1_stats.csv", index=False)

df2 = pd.read_sql("SELECT * FROM champ_item2_stats", con)
df2.to_csv("data/exports/champ_item2_stats.csv", index=False)

df3 = pd.read_sql("SELECT * FROM champ_item3_stats", con)
df3.to_csv("data/exports/champ_item3_stats.csv", index=False)

con.close()
print("Exported to data/exports/")
