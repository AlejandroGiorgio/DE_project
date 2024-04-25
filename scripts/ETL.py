import os
import json
import requests
import pandas as pd
import psycopg2
import smtplib
from sqlalchemy import create_engine
from dotenv import load_dotenv
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from airflow.models import Variable

# Cargar variables de entorno
load_dotenv()

# Definir las credenciales de Redshift
REDSHIFT_CREDENTIALS = {
    "host": os.environ["REDSHIFT_HOST"],
    "dbname": os.environ["REDSHIFT_DATABASE"],
    "user": os.environ["REDSHIFT_USER"],
    "password": os.environ["REDSHIFT_PWD"],
    "port": os.environ["REDSHIFT_PORT"]
}

def create_redshift_engine():
    """Crear el motor SQLAlchemy para Redshift."""
    return create_engine(
        f"postgresql+psycopg2://{REDSHIFT_CREDENTIALS['user']}:{REDSHIFT_CREDENTIALS['password']}@"
        f"{REDSHIFT_CREDENTIALS['host']}:{REDSHIFT_CREDENTIALS['port']}/{REDSHIFT_CREDENTIALS['dbname']}"
    )

def create_table(conn):
    """Crear la tabla en Redshift si no existe."""
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
    cur = conn.cursor()
    cur.execute(create_table_command)
    conn.commit()

def fetch_data_from_api():
    """Extraer datos de la API."""
    base_url = "https://data.sfgov.org/resource/wg3w-h783.json"
    offset = 0
    limit = 50000
    df = pd.DataFrame()

    while True:
        url = f"{base_url}?$limit={limit}&$offset={offset}"
        response = requests.get(url)
        if response.status_code == 200:
            data = pd.read_json(response.text)
            if data.empty:
                break
            df = pd.concat([df, data], ignore_index=True)
            offset += limit
        else:
            raise Exception(f"Failed to fetch data. Status code: {response.status_code}")
    return df

def transform_data(df):
    """Transformar los datos extraídos."""
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
    return df


def load_data_to_redshift(df, engine):
    """Cargar los datos transformados a Redshift."""
    df.to_sql("sf_police_incidents", engine, if_exists="append", index=False)



# Configuración de correo electrónico
smtp_server = 'smtp.gmail.com'
smtp_port = 587
sender_email = 'alegior7@gmail.com'
password = Variable.get("secret_pass_gmail")  # Asegúrate de haber configurado esta Variable en Airflow

def send_email(subject, body_text):
    try:
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = sender_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body_text, 'plain'))

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, password)
            server.send_message(msg)

        print('El email fue enviado correctamente.')

    except Exception as exception:
        print(f'El email no se pudo enviar. Error: {exception}')


def check_threshold_and_alert(df):
    """Verificar si algún valor sobrepasa un límite y enviar alertas."""
    thresholds = {
        'incident_number': 50000,  
    }

    alerts = []

    for column, threshold in thresholds.items():
        # Verificar si algún valor en la columna excede el umbral
        if (df[column] > threshold).any():
            alerts.append(f"El valor en la columna '{column}' ha superado el umbral de {threshold}.")

    # Si hay alertas, enviar un correo electrónico
    if alerts:
        subject = "Alerta de umbral superado"
        message = "\n".join(alerts)
        send_email(subject, message)


def close_connection(conn):
    """Cerrar la conexión a la base de datos."""
    conn.close()