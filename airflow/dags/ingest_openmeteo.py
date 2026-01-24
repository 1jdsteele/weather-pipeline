from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator



# +++++++++++ Configuration (MVP) +++++++++++++


# ClickHouse connection (Docker service name)
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "weather")

# Location to ingest (default: pasadena)
LOCATION_NAME = os.getenv("WEATHER_LOCATION_NAME", "pasadena-ca")
LAT = float(os.getenv("WEATHER_LAT", "34.1478"))
LON = float(os.getenv("WEATHER_LON", "-118.1445"))

# Data source label stored in ClickHouse
SOURCE = os.getenv("WEATHER_SOURCE", "open-meteo")


def build_open_meteo_url(lat: float, lon: float) -> str:
    """
    Open-Meteo endpoint for current weather.
    Docs: https://open-meteo.com/
    """
    return (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        "&current_weather=true"
    )


#  ++++++++++ Task logic +++++++++++
def fetch_and_ingest_weather(**context) -> None:
    """
    1) Fetch Open-Meteo JSON
    2) Insert a single raw row into ClickHouse: weather.raw_ingest
    """

    # 1) Fetch data
    url = build_open_meteo_url(LAT, LON)
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    payload_dict = resp.json()

    # Add a couple helpful metadata fields inside payload
    payload_dict["_meta"] = {
        "requested_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": SOURCE,
        "location_name": LOCATION_NAME,
        "lat": LAT,
        "lon": LON,
    }

    payload_str = json.dumps(payload_dict)

    # 2) Insert into ClickHouse (via HTTP interface) using JSONEachRow (found out was necessary)
    row = {
        "source": SOURCE,
        "location": LOCATION_NAME,
        "lat": LAT,
        "lon": LON,
        "payload": payload_str,  # JSON string
    }

    full_sql = "INSERT INTO weather.raw_ingest FORMAT JSONEachRow\n" + json.dumps(row) + "\n"

    ch_url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
    params = {
        "database": CLICKHOUSE_DATABASE,
        "user": CLICKHOUSE_USER,
        "password": CLICKHOUSE_PASSWORD,
    }

    ch_resp = requests.post(ch_url, params=params, data=full_sql.encode("utf-8"), timeout=15)
    ch_resp.raise_for_status()



# +++++++++ THE DAG +++++++++++
default_args = {
    "owner": "jake",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ingest_open_meteo",
    description="Fetch current weather from Open-Meteo and ingest raw payload into ClickHouse",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["weather", "ingestion", "clickhouse"],
) as dag:
    ingest_task = PythonOperator(
        task_id="fetch_and_ingest_weather",
        python_callable=fetch_and_ingest_weather,
    )
