import schedule
import time
import requests
import pandas as pd
from sqlalchemy import create_engine
import json
import io

# Tus credenciales de acceso a Redshift
host = "your_host"
port = "your_port"
dbname = "your_dbname"
user = "your_user"
password = "your_password"

# Crear el motor SQLAlchemy
engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
)

def job():
    base_url = "https://data.sfgov.org/resource/wg3w-h783.json"
    offset = 0
    limit = 1000
    df = pd.DataFrame()

    while True:
        url = f"{base_url}?$limit={limit}&$offset={offset}"
        response = requests.get(url)

        if response.status_code == 200:
            data = pd.read_json(io.StringIO(response.text))

            if data.empty:
                break

            df = pd.concat([df, data], ignore_index=True)
            offset += limit
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            break

    # Realizar el proceso de ETL
    from constants import date_columns, string_columns

    for col in date_columns:
        df[col] = pd.to_datetime(df[col])

    for col in string_columns:
        df[col] = df[col].str.lower().str.strip()

    threshold = int(0.6 * len(df))
    df = df.dropna(thresh=threshold, axis=1)

    df.columns = df.columns.str.replace(':', '')
    df.columns = df.columns.str.replace('@', '')

    df['point'] = df['point'].apply(json.dumps)

    # Eliminar duplicados
    df = df.drop_duplicates()

    # Cargar los datos a Redshift
    df.to_sql("sf_police_incidents", engine, if_exists="append", index=False)

# Programar la función para que se ejecute cada cierto tiempo
# En este caso, se ejecutará cada día
schedule.every().day.at("00:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)