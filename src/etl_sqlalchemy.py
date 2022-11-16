import logging as log
import pandas as pd
import sqlalchemy as db
from sqlalchemy import Integer, Text

# logging config
log.basicConfig(level=log.DEBUG,
                format='%(asctime)s %(message)s'
                )

# sqlalchemy version, engine creation
try:
    engine = db.create_engine('sqlite:///database/olympics.db')
    sqla_connection = engine.connect()
except Exception:
    log.error("version sqlalchemy connection failed", exc_info=True)


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

# Upload data to "olympics" database in table "medals"
df_gold_usa.to_sql(
        'medals',
        engine,
        if_exists='replace',
        index=False,
        chunksize=500,
        dtype={
            "Year": Integer,
            "City": Text,
            "Sport": Text,
            "Discipline":  Text,
            "NOC": Text,
            "Event": Text,
            "Event gender": Text,
            "Medal": Text

        })

# Select all data from medals table, pandas version
query = "SELECT * FROM medals"
data = pd.read_sql(query, con=sqla_connection)
print(data)
