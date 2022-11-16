import logging as log
import pandas as pd
import sqlite3

# logging config
log.basicConfig(level=log.DEBUG,
                format='%(asctime)s %(message)s'
                )

# sqlite connection version
try:
    conn = sqlite3.connect('database/olympics.db')
    # cursor
    cursor = conn.cursor()
except Exception:
    log.error("version sqlite connection failed", exc_info=True)

# url where dataset is
url: str = "http://winterolympicsmedals.com/medals.csv "

# Get the data from url
try:
    df_medals = pd.read_csv(url)
except Exception:
    log.error("url read failed", exc_info=True)


# Filter data and add to a new dataframe
df_gold_usa = df_medals[(df_medals["NOC"] == "USA") & (df_medals["Medal"]
                        == "Gold") & (df_medals["Year"] >= 1950)]

# Create table medals2
query = """CREATE TABLE IF NOT EXISTS medals2 (
            Year INTEGER,
            City TEXT,
            Sport TEXT,
            Discipline TEXT,
            NOC TEXT,
            Event TEXT,
            Event_gender TEXT,
            Medal TEXT
            )"""
cursor.execute(query)
conn.commit()

# Insert
df_gold_usa.to_sql(
        'medals2',
        conn,
        if_exists='replace',
        index=False,
        )

# select all data from medals, sqlite version
query_select = "SELECT * FROM medals2"
cursor.execute(query_select)
data = cursor.fetchall()
# data = pd.DataFrame(data)
print(data)
