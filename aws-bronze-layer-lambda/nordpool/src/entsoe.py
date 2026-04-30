import os
import json
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
import boto3
from botocore.exceptions import ClientError
from parameters import *
# =========================
# AWS / CONFIG
# =========================

URL = "https://web-api.tp.entsoe.eu/api"
AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")
BUCKET_NAME = os.getenv("BUCKET_NAME", "data-lake-energy-trade")
SSM_TOKEN_PARAM = os.getenv("ENTSOE_TOKEN_PARAM", "/entsoe/security_token")

s3 = boto3.client("s3", region_name=AWS_REGION)


def get_ssm_parameter(name: str, with_decryption: bool = True, region_name: str = AWS_REGION) -> str:
    ssm = boto3.client("ssm", region_name=region_name)
    try:
        response = ssm.get_parameter(Name=name, WithDecryption=with_decryption)
        return response["Parameter"]["Value"]
    except ClientError as e:
        raise RuntimeError(f"Failed to fetch SSM parameter '{name}': {e}")


TOKEN = get_ssm_parameter(SSM_TOKEN_PARAM)


ALL_DATASETS = list(DATASET_CONFIGS.keys())


# =========================
# HELPERS
# =========================



def get_hourly_window():
    now = datetime.now(timezone.utc)
    period_end_dt = now.replace(minute=0, second=0, microsecond=0)
    period_start_dt = period_end_dt - timedelta(hours=1)

    return api_ts(period_start_dt), api_ts(period_end_dt)

def get_last_closed_hour_window():
    now = datetime.now(timezone.utc)
    current_hour = now.replace(minute=0, second=0, microsecond=0)
    period_end_dt = current_hour
    period_start_dt = current_hour - timedelta(hours=1)
    return api_ts(period_start_dt), api_ts(period_end_dt)



def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def api_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y%m%d%H%M")


def resolution_to_timedelta(resolution: str) -> timedelta:
    mapping = {
        "PT15M": timedelta(minutes=15),
        "PT30M": timedelta(minutes=30),
        "PT60M": timedelta(hours=1),
        "PT1H": timedelta(hours=1),
        "P1D": timedelta(days=1),
        "P7D": timedelta(days=7),
    }
    if resolution not in mapping:
        raise ValueError(f"Unsupported resolution: {resolution}")
    return mapping[resolution]


def period_position_to_timestamp(start_iso: str, resolution: str, position: int) -> str:
    start_dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    delta = resolution_to_timedelta(resolution)
    ts = start_dt + (position - 1) * delta
    return iso_z(ts)


def build_params(dataset_name: str, area_code: str, period_start: str, period_end: str) -> dict:
    cfg = DATASET_CONFIGS[dataset_name]

    params = {
        "securityToken": TOKEN,
        "documentType": cfg["document_type"],
        "periodStart": period_start,
        "periodEnd": period_end,
    }

    if cfg["process_type"]:
        params["processType"] = cfg["process_type"]

    domain_mode = cfg["domain_mode"]

    if domain_mode == "out_bzn":
        params["outBiddingZone_Domain"] = area_code
    elif domain_mode == "in_domain":
        params["in_Domain"] = area_code
    elif domain_mode == "in_out_same":
        params["in_Domain"] = area_code
        params["out_Domain"] = area_code
    else:
        raise ValueError(f"Unsupported domain_mode: {domain_mode}")

    return params


def get_root_and_ns(xml_text: str):
    root = ET.fromstring(xml_text)
    ns_uri = root.tag.split("}")[0].strip("{")
    ns = {"ns": ns_uri}
    root_name = root.tag.split("}")[-1]
    return root, ns, root_name


def parse_acknowledgement(
    xml_text: str,
    dataset_name: str,
    country_code: str,
    country: str,
    area_code: str,
    area_name: str,
    ingested_at: str,
    request_period_start: str,
    request_period_end: str,
) -> list[dict]:
    root, ns, _ = get_root_and_ns(xml_text)

    return [{
        "ingested_at": ingested_at,
        "source": "entsoe",
        "dataset": dataset_name,
        "country_code": country_code,
        "country": country,
        "area_code": area_code,
        "area_name": area_name,
        "status": "no_data",
        "response_type": "Acknowledgement_MarketDocument",
        "request_period_start": request_period_start,
        "request_period_end": request_period_end,
        "created_datetime": root.findtext("ns:createdDateTime", default="", namespaces=ns),
        "reason_code": root.findtext("ns:Reason/ns:code", default="", namespaces=ns),
        "reason_text": root.findtext("ns:Reason/ns:text", default="", namespaces=ns),
    }]


def parse_gl_market_document(
    xml_text: str,
    dataset_name: str,
    country_code: str,
    country: str,
    area_code: str,
    area_name: str,
    ingested_at: str,
    request_period_start: str,
    request_period_end: str,
) -> list[dict]:
    root, ns, _ = get_root_and_ns(xml_text)

    document_mrid = root.findtext("ns:mRID", default="", namespaces=ns)
    revision_number = root.findtext("ns:revisionNumber", default="", namespaces=ns)
    document_type = root.findtext("ns:type", default="", namespaces=ns)
    process_type = root.findtext("ns:process.processType", default="", namespaces=ns)
    created_datetime = root.findtext("ns:createdDateTime", default="", namespaces=ns)
    doc_time_start = root.findtext("ns:time_Period.timeInterval/ns:start", default="", namespaces=ns)
    doc_time_end = root.findtext("ns:time_Period.timeInterval/ns:end", default="", namespaces=ns)

    rows = []

    for ts in root.findall("ns:TimeSeries", ns):
        timeseries_mrid = ts.findtext("ns:mRID", default="", namespaces=ns)
        business_type = ts.findtext("ns:businessType", default="", namespaces=ns)
        object_aggregation = ts.findtext("ns:objectAggregation", default="", namespaces=ns)
        unit = ts.findtext("ns:quantity_Measure_Unit.name", default="", namespaces=ns)

        in_domain = ts.findtext("ns:inBiddingZone_Domain.mRID", default="", namespaces=ns)
        out_domain = ts.findtext("ns:outBiddingZone_Domain.mRID", default="", namespaces=ns)
        price_unit = ts.findtext("ns:currency_Unit.name", default="", namespaces=ns)
        price_measure_unit = ts.findtext("ns:price_Measure_Unit.name", default="", namespaces=ns)
        psr_type = ts.findtext("ns:MktPSRType/ns:psrType", default="", namespaces=ns)

        for period in ts.findall("ns:Period", ns):
            period_start = period.findtext("ns:timeInterval/ns:start", default="", namespaces=ns)
            period_end = period.findtext("ns:timeInterval/ns:end", default="", namespaces=ns)
            resolution = period.findtext("ns:resolution", default="", namespaces=ns)

            for point in period.findall("ns:Point", ns):
                position_text = point.findtext("ns:position", default="", namespaces=ns)
                quantity_text = point.findtext("ns:quantity", default="", namespaces=ns)
                price_text = point.findtext("ns:price.amount", default="", namespaces=ns)

                try:
                    position = int(position_text)
                except (TypeError, ValueError):
                    position = None

                timestamp_utc = None
                if position is not None and period_start and resolution:
                    try:
                        timestamp_utc = period_position_to_timestamp(period_start, resolution, position)
                    except Exception:
                        timestamp_utc = None

                value = None
                value_type = None
                if quantity_text not in ("", None):
                    value = float(quantity_text)
                    value_type = "quantity"
                elif price_text not in ("", None):
                    value = float(price_text)
                    value_type = "price"

                rows.append({
                    "ingested_at": ingested_at,
                    "source": "entsoe",
                    "dataset": dataset_name,
                    "country_code": country_code,
                    "country": country,
                    "area_code": area_code,
                    "area_name": area_name,
                    "timestamp_utc": timestamp_utc,
                    "document_mrid": document_mrid,
                    "revision_number": revision_number,
                    "document_type": document_type,
                    "process_type": process_type,
                    "created_datetime": created_datetime,
                    "document_time_start": doc_time_start,
                    "document_time_end": doc_time_end,
                    "request_period_start": request_period_start,
                    "request_period_end": request_period_end,
                    "timeseries_mrid": timeseries_mrid,
                    "business_type": business_type,
                    "business_type_label": BUSINESS_TYPE_LABELS.get(business_type),
                    "object_aggregation": object_aggregation,
                    "in_domain": in_domain,
                    "out_domain": out_domain,
                    "period_start": period_start,
                    "period_end": period_end,
                    "resolution": resolution,
                    "position": position,
                    "value": value,
                    "value_type": value_type,
                    "quantity_mw": float(quantity_text) if quantity_text not in ("", None) else None,
                    "price_amount": float(price_text) if price_text not in ("", None) else None,
                    "unit": unit,
                    "currency_unit": price_unit,
                    "price_measure_unit": price_measure_unit,
                    "psr_type": psr_type or None,
                    "technology": PSR_LABELS.get(psr_type) if psr_type else None,
                    "status": "ok",
                })

    if not rows:
        rows.append({
            "ingested_at": ingested_at,
            "source": "entsoe",
            "dataset": dataset_name,
            "country_code": country_code,
            "country": country,
            "area_code": area_code,
            "area_name": area_name,
            "document_mrid": document_mrid,
            "revision_number": revision_number,
            "document_type": document_type,
            "process_type": process_type,
            "created_datetime": created_datetime,
            "document_time_start": doc_time_start,
            "document_time_end": doc_time_end,
            "request_period_start": request_period_start,
            "request_period_end": request_period_end,
            "status": "ok_but_empty_points",
        })

    return rows


def fetch_dataset_xml(dataset_name: str, area_code: str, period_start: str, period_end: str) -> str:
    params = build_params(dataset_name, area_code, period_start, period_end)
    r = requests.get(URL, params=params, timeout=60)
    r.raise_for_status()
    return r.text


def fetch_and_parse_dataset(
    dataset_name: str,
    country_code: str,
    country: str,
    area_code: str,
    area_name: str,
    period_start: str,
    period_end: str,
    ingested_at: str,
) -> list[dict]:
    xml_text = fetch_dataset_xml(dataset_name, area_code, period_start, period_end)
    root, _, root_name = get_root_and_ns(xml_text)

    if root_name == "Acknowledgement_MarketDocument":
        return parse_acknowledgement(
            xml_text=xml_text,
            dataset_name=dataset_name,
            country_code=country_code,
            country=country,
            area_code=area_code,
            area_name=area_name,
            ingested_at=ingested_at,
            request_period_start=period_start,
            request_period_end=period_end,
        )

    if root_name == "GL_MarketDocument":
        return parse_gl_market_document(
            xml_text=xml_text,
            dataset_name=dataset_name,
            country_code=country_code,
            country=country,
            area_code=area_code,
            area_name=area_name,
            ingested_at=ingested_at,
            request_period_start=period_start,
            request_period_end=period_end,
        )

    return [{
        "ingested_at": ingested_at,
        "source": "entsoe",
        "dataset": dataset_name,
        "country_code": country_code,
        "country": country,
        "area_code": area_code,
        "area_name": area_name,
        "status": "unknown_response_type",
        "response_type": root_name,
        "request_period_start": period_start,
        "request_period_end": period_end,
        "raw_xml_preview": xml_text[:1000],
    }]


def write_jsonl_to_s3(dataset_name: str, rows: list[dict], ingested_at_dt: datetime) -> str:
    year = ingested_at_dt.strftime("%Y")
    month = ingested_at_dt.strftime("%m")
    day = ingested_at_dt.strftime("%d")
    hour = ingested_at_dt.strftime("%H")
    ts_for_filename = ingested_at_dt.strftime("%Y-%m-%dT%H-%M-%SZ")

    key = (
        f"bronze/entsoe/{dataset_name}/"
        f"year={year}/month={month}/day={day}/hour={hour}/"
        f"{dataset_name}_{ts_for_filename}.jsonl"
    )

    body = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    return key


def should_use_dataset(area: dict, dataset_name: str) -> bool:
    use_for = area.get("use_for", [])
    return "all_main_market_datasets" in use_for or dataset_name in use_for


def collect_dataset_rows_for_all_areas(
    dataset_name: str,
    period_start: str,
    period_end: str,
    ingested_at: str,
) -> list[dict]:
    all_rows = []

    for country_cfg in entsoe_country_params:
        country_code = country_cfg["country_code"]
        country = country_cfg["country"]

        for area in country_cfg["entsoe_areas"]:
            if not should_use_dataset(area, dataset_name):
                continue

            area_code = area["area_code"]
            area_name = area["area_name"]

            try:
                rows = fetch_and_parse_dataset(
                    dataset_name=dataset_name,
                    country_code=country_code,
                    country=country,
                    area_code=area_code,
                    area_name=area_name,
                    period_start=period_start,
                    period_end=period_end,
                    ingested_at=ingested_at,
                )
                all_rows.extend(rows)
            except Exception as e:
                all_rows.append({
                    "ingested_at": ingested_at,
                    "source": "entsoe",
                    "dataset": dataset_name,
                    "country_code": country_code,
                    "country": country,
                    "area_code": area_code,
                    "area_name": area_name,
                    "status": "request_failed",
                    "request_period_start": period_start,
                    "request_period_end": period_end,
                    "error": str(e),
                })

    return all_rows


def run_entsoe_bronze_ingestion(period_start: str, period_end: str) -> dict:
    ingested_at_dt = datetime.now(timezone.utc)
    ingested_at = iso_z(ingested_at_dt)

    written_files = []

    for dataset_name in ALL_DATASETS:
        rows = collect_dataset_rows_for_all_areas(
            dataset_name=dataset_name,
            period_start=period_start,
            period_end=period_end,
            ingested_at=ingested_at,
        )
        s3_key = write_jsonl_to_s3(dataset_name, rows, ingested_at_dt)
        written_files.append({
            "dataset": dataset_name,
            "row_count": len(rows),
            "s3_key": s3_key,
        })

    return {
        "status": "ok",
        "ingested_at": ingested_at,
        "bucket": BUCKET_NAME,
        "period_start": period_start,
        "period_end": period_end,
        "files_written": written_files,
    }

