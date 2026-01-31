from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)

# ++++++++++ Configuration ++++++++++
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "weather")

LOCATION_NAME = os.getenv("WEATHER_LOCATION_NAME", "pasadena-ca")
LAT = float(os.getenv("WEATHER_LAT", "34.1478"))
LON = float(os.getenv("WEATHER_LON", "-118.1445"))

SOURCE = os.getenv("WEATHER_SOURCE", "open-meteo")


def build_open_meteo_url(lat: float, lon: float) -> str:
    return (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        "&current_weather=true"
    )


# ---------- Validation helpers ----------
def validate_lat_lon(lat: float, lon: float) -> None:
    if not (-90.0 <= lat <= 90.0):
        raise ValueError(f"Invalid latitude {lat}. Expected -90..90.")
    if not (-180.0 <= lon <= 180.0):
        raise ValueError(f"Invalid longitude {lon}. Expected -180..180.")


def validate_open_meteo_payload(payload: Dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Open-Meteo payload is not a JSON object.")

    if "current_weather" not in payload:
        raise ValueError("Open-Meteo response missing 'current_weather'.")

    cw = payload["current_weather"]
    if not isinstance(cw, dict):
        raise ValueError("'current_weather' is not an object.")

    required = ["temperature", "time"]
    missing = [k for k in required if k not in cw]
    if missing:
        raise ValueError(f"'current_weather' missing required keys: {missing}")


def ensure_json_serializable(obj: Any, label: str) -> None:
    try:
        json.dumps(obj)
    except (TypeError, ValueError) as e:
        raise ValueError(f"{label} is not JSON-serializable: {e}") from e


def require_nonempty_str(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string.")
    return value


def validate_raw_ingest_row_shape(row: Dict[str, Any]) -> None:
    expected = {"source", "location", "lat", "lon", "payload"}
    extra = set(row.keys()) - expected
    missing = expected - set(row.keys())
    if missing:
        raise ValueError(f"Row missing required keys: {sorted(missing)}")
    if extra:
        raise ValueError(f"Row has unexpected keys: {sorted(extra)}")


default_args = {
    "owner": "jake",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}


#++++++++++++++=the dag++++++++++++++++
@dag(
    dag_id="ingest_open_meteo",
    description="Fetch current weather from Open-Meteo and ingest raw payload into ClickHouse",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["weather", "ingestion", "clickhouse"],
)
def ingest_open_meteo_taskflow():
    @task(task_id="build_url")
    def build_url(lat: float, lon: float) -> str:
        validate_lat_lon(lat, lon)
        url = build_open_meteo_url(lat, lon)
        require_nonempty_str(url, "Open-Meteo URL")
        log.info("Built URL for location=%s lat=%s lon=%s", LOCATION_NAME, lat, lon)
        return url

    @task(task_id="fetch_weather", retries=5, retry_delay=timedelta(minutes=2))
    def fetch_weather(url: str) -> Dict[str, Any]:
        url = require_nonempty_str(url, "url")
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            payload = resp.json()
        except requests.exceptions.Timeout as e:
            raise AirflowException(f"fetch_weather timeout: url={url}") from e
        except requests.exceptions.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            body = (getattr(e.response, "text", "") or "")[:500]
            raise AirflowException(f"fetch_weather HTTPError: status={status} url={url} body={body}") from e
        except requests.exceptions.RequestException as e:
            raise AirflowException(f"fetch_weather request failed: url={url} err={e}") from e
        except ValueError as e:
            raise AirflowException(f"fetch_weather invalid JSON response: url={url} err={e}") from e

        validate_open_meteo_payload(payload)
        log.info("Fetched Open-Meteo payload and passed validation.")
        return payload

    @task(task_id="add_metadata")
    def add_metadata(payload: Dict[str, Any]) -> Dict[str, Any]:
        validate_open_meteo_payload(payload)
        enriched: Dict[str, Any] = dict(payload)
        enriched["_meta"] = {
            "requested_at_utc": datetime.now(timezone.utc).isoformat(),
            "source": SOURCE,
            "location_name": LOCATION_NAME,
            "lat": LAT,
            "lon": LON,
        }
        ensure_json_serializable(enriched, "enriched payload")
        return enriched

    @task(task_id="build_clickhouse_insert")
    def build_clickhouse_insert(enriched_payload: Dict[str, Any]) -> str:
        if "_meta" not in enriched_payload:
            raise ValueError("enriched_payload missing _meta (expected after add_metadata).")

        payload_str = json.dumps(enriched_payload)

        row = {
            "source": SOURCE,
            "location": LOCATION_NAME,
            "lat": LAT,
            "lon": LON,
            "payload": payload_str,
        }
        validate_raw_ingest_row_shape(row)
        ensure_json_serializable(row, "raw_ingest row")

        full_sql = (
            f"INSERT INTO {CLICKHOUSE_DATABASE}.raw_ingest FORMAT JSONEachRow\n"
            + json.dumps(row)
            + "\n"
        )
        require_nonempty_str(full_sql, "full_sql")
        return full_sql

    @task(task_id="post_to_clickhouse", retries=5, retry_delay=timedelta(minutes=2))
    def post_to_clickhouse(full_sql: str) -> None:
        full_sql = require_nonempty_str(full_sql, "full_sql")

        ch_url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
        params = {
            "database": CLICKHOUSE_DATABASE,
            "user": CLICKHOUSE_USER,
            "password": CLICKHOUSE_PASSWORD,
        }

        try:
            resp = requests.post(ch_url, params=params, data=full_sql.encode("utf-8"), timeout=15)
            resp.raise_for_status()
        except requests.exceptions.Timeout as e:
            raise AirflowException(f"post_to_clickhouse timeout: url={ch_url} db={CLICKHOUSE_DATABASE}") from e
        except requests.exceptions.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            body = (getattr(e.response, "text", "") or "")[:800]
            raise AirflowException(
                f"post_to_clickhouse HTTPError: status={status} url={ch_url} db={CLICKHOUSE_DATABASE} body={body}"
            ) from e
        except requests.exceptions.RequestException as e:
            raise AirflowException(f"post_to_clickhouse request failed: url={ch_url} err={e}") from e

        log.info("Inserted 1 row into %s.raw_ingest for location=%s", CLICKHOUSE_DATABASE, LOCATION_NAME)

    # the 5 nodes of this dag
    url = build_url(LAT, LON)
    raw_payload = fetch_weather(url)
    enriched_payload = add_metadata(raw_payload)
    full_sql = build_clickhouse_insert(enriched_payload)
    post_to_clickhouse(full_sql)


dag = ingest_open_meteo_taskflow()
