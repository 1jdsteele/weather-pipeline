from __future__ import annotations
import hashlib
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context

log = logging.getLogger(__name__)

# ++++++++ Configuration +++++++++++++
LOCATION_NAME = os.getenv("WEATHER_LOCATION_NAME", "pasadena-ca")
LAT = float(os.getenv("WEATHER_LAT", "34.1478"))
LON = float(os.getenv("WEATHER_LON", "-118.1445"))
SOURCE = os.getenv("WEATHER_SOURCE", "open-meteo")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "weather-raw")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")

# We created the manifest in the `weather` DB
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "weather")
CLICKHOUSE_MANIFEST_TABLE = os.getenv(
    "CLICKHOUSE_MANIFEST_TABLE", "minio_weather_manifest"
)


def build_open_meteo_url(lat: float, lon: float) -> str:
    return (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        "&current_weather=true"
    )


# ++++++++ validation helpers ++++++++
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


def s3_client():
    # it did not like this globally, DO NOT MOVE TO GLOBAL SCOPE
    import boto3

    scheme = "https" if MINIO_SECURE else "http"
    return boto3.client(
        "s3",
        endpoint_url=f"{scheme}://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


def canonical_object_key(location: str, logical_dt: datetime) -> str:
    # one file per hour
    dt = logical_dt.astimezone(timezone.utc)
    date_part = dt.strftime("%Y-%m-%d")
    hour_part = dt.strftime("%H")
    return f"open-meteo/location={location}/date={date_part}/hour={hour_part}.json"


default_args = {"owner": "jake", "retries": 5, "retry_delay": timedelta(minutes=2)}


@dag(
    dag_id="ingest_open_meteo_to_minio",
    description="Fetch Open-Meteo current weather and write enriched raw JSON to MinIO (canonical hourly object)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["weather", "ingestion", "minio", "bronze"],
)
def ingest_open_meteo_to_minio():
    @task(task_id="build_url")
    def build_url() -> str:
        validate_lat_lon(LAT, LON)
        url = build_open_meteo_url(LAT, LON)
        log.info("Built URL for %s lat=%s lon=%s", LOCATION_NAME, LAT, LON)
        return url

    @task(task_id="fetch_weather", retries=5, retry_delay=timedelta(minutes=2))
    def fetch_weather(url: str) -> Dict[str, Any]:
        # DO NOT MOVE GLOBALLY, WILL BREAK
        import requests

        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            payload = resp.json()
        except requests.exceptions.Timeout as e:
            raise AirflowException(f"fetch_weather timeout: url={url}") from e
        except requests.exceptions.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            body = (getattr(e.response, "text", "") or "")[:500]
            raise AirflowException(
                f"fetch_weather HTTPError: status={status} url={url} body={body}"
            ) from e
        except requests.exceptions.RequestException as e:
            raise AirflowException(
                f"fetch_weather request failed: url={url} err={e}"
            ) from e
        except ValueError as e:
            raise AirflowException(
                f"fetch_weather invalid JSON response: url={url} err={e}"
            ) from e

        validate_open_meteo_payload(payload)
        return payload

    @task(task_id="add_metadata")
    def add_metadata(payload: Dict[str, Any]) -> Dict[str, Any]:
        context = get_current_context()
        enriched: Dict[str, Any] = dict(payload)
        enriched["_meta"] = {
            "requested_at_utc": datetime.now(timezone.utc).isoformat(),
            "source": SOURCE,
            "location_name": LOCATION_NAME,
            "lat": LAT,
            "lon": LON,
            "airflow_run_id": context["run_id"],
            "airflow_try_number": context["ti"].try_number,
            "logical_date_utc": context["logical_date"].isoformat(),
        }
        return enriched

    @task(task_id="write_to_minio")
    def write_to_minio(enriched_payload: Dict[str, Any]) -> Dict[str, Any]:
        context = get_current_context()
        logical_dt: datetime = context["logical_date"]

        key = canonical_object_key(LOCATION_NAME, logical_dt)

        payload_bytes = json.dumps(
            enriched_payload, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        sha = hashlib.sha256(payload_bytes).hexdigest()

        client = s3_client()
        client.put_object(
            Bucket=MINIO_BUCKET,
            Key=key,
            Body=payload_bytes,
            ContentType="application/json",
            Metadata={"sha256": sha},
        )

        log.info(
            "Wrote MinIO object: bucket=%s key=%s sha256=%s", MINIO_BUCKET, key, sha
        )

        cw = enriched_payload["current_weather"]
        return {
            "bucket": MINIO_BUCKET,
            "object_key": key,
            "payload_sha256": sha,
            "object_bytes": len(payload_bytes),
            "observed_time": cw["time"],
            "temperature_c": cw["temperature"],
        }

    @task(
        task_id="write_manifest_to_clickhouse",
        retries=5,
        retry_delay=timedelta(minutes=2),
    )
    def write_manifest_to_clickhouse(minio_result: Dict[str, Any]) -> None:
        # DO NOT MOVE GlOBALLY
        import requests

        context = get_current_context()
        logical_dt: datetime = context["logical_date"]

        hour_utc = logical_dt.astimezone(timezone.utc).replace(
            minute=0, second=0, microsecond=0
        )

        row = {
            "source": SOURCE,
            "location_name": LOCATION_NAME,
            "hour_utc": hour_utc.strftime("%Y-%m-%d %H:%M:%S"),
            "bucket": minio_result["bucket"],
            "object_key": minio_result["object_key"],
            "payload_sha256": minio_result["payload_sha256"],
            "object_bytes": int(minio_result.get("object_bytes", 0)),
            "airflow_run_id": context["run_id"],
            "airflow_try_number": int(context["ti"].try_number),
        }

        log.info("Manifest row JSON: %s", json.dumps(row))

        insert_sql = f"""
        INSERT INTO {CLICKHOUSE_DATABASE}.{CLICKHOUSE_MANIFEST_TABLE}
        (
            source, location_name, hour_utc,
            bucket, object_key,
            payload_sha256, object_bytes,
            airflow_run_id, airflow_try_number
        )
        FORMAT JSONEachRow
        """

        url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/?database={CLICKHOUSE_DATABASE}"
        data = json.dumps(row) + "\n"

        try:
            resp = requests.post(
                url,
                auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
                params={"query": insert_sql},
                data=data,
                timeout=15,
            )
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            body = (getattr(resp, "text", "") or "")[:800] if "resp" in locals() else ""
            raise AirflowException(
                f"ClickHouse manifest insert failed: err={e} body={body}"
            ) from e

        log.info(
            "Wrote manifest row: location=%s hour_utc=%s key=%s",
            LOCATION_NAME,
            row["hour_utc"],
            row["object_key"],
        )

    url = build_url()
    payload = fetch_weather(url)
    enriched = add_metadata(payload)
    minio_result = write_to_minio(enriched)
    write_manifest_to_clickhouse(minio_result)


dag = ingest_open_meteo_to_minio()
