import requests
import pandas as pd
from sqlalchemy import create_engine
import os
import json
import io
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Define your Redshift credentials
host = os.environ["REDSHIFT_HOST"]
dbname = os.environ["REDSHIFT_DATABASE"]
user = os.environ["REDSHIFT_USER"]
password = os.environ["REDSHIFT_PWD"]
port = os.environ["REDSHIFT_PORT"]

# Crear el motor SQLAlchemy
engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
)

# Crear una conexión a Redshift
conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    port=port,
    host=host
)

def job():
    try:
        # Crear un cursor
        cur = conn.cursor()

        # Definir la sentencia DDL
        create_table_command = """
        CREATE TABLE IF NOT EXISTS sf_police_incidents (
            incident_datetime TIMESTAMP,
            incident_date DATE,
            incident_time TIME,
            incident_year INT,
            incident_day_of_week VARCHAR(255),
            report_datetime TIMESTAMP,
            row_id BIGINT,
            incident_id INT,
            incident_number INT,
            report_type_code VARCHAR(255),
            report_type_description VARCHAR(255),
            incident_code INT,
            incident_category VARCHAR(255),
            incident_subcategory VARCHAR(255),
            incident_description VARCHAR(255),
            resolution VARCHAR(255),
            police_district VARCHAR(255),
            cad_number FLOAT,
            intersection VARCHAR(255),
            cnn FLOAT,
            analysis_neighborhood VARCHAR(255),
            supervisor_district FLOAT,
            supervisor_district_2012 FLOAT,
            latitude FLOAT,
            longitude FLOAT,
            point VARCHAR(255),
            computed_region_26cr_cadq FLOAT,
            computed_region_qgnn_b9vv FLOAT,
            computed_region_jwn9_ihcz FLOAT
        )
        """

        # Ejecutar la sentencia DDL
        cur.execute(create_table_command)

        # Confirmar los cambios
        conn.commit()

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
        from scripts.constants import date_columns, string_columns

        for col in date_columns:
            df[col] = pd.to_datetime(df[col])

        for col in string_columns:
            df[col] = df[col].str.lower().str.strip()

        threshold = int(0.6 * len(df))
        df = df.dropna(thresh=threshold, axis=1)

        df.columns = df.columns.str.replace(":", "")
        df.columns = df.columns.str.replace("@", "")

        df["point"] = df["point"].apply(json.dumps)

        # Eliminar duplicados
        df = df.drop_duplicates()

        # Cargar los datos a Redshift
        df.to_sql("sf_police_incidents", engine, if_exists="append", index=False)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Cerrar la conexión
        conn.close()

job()
