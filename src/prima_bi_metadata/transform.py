from datetime import datetime, timezone
import pandas as pd

def parse_timestamp(ts):
    if ts is None:
        return None
    try:
        # assume ISO format; adapt if API uses different format
        return pd.to_datetime(ts).tz_convert(None).tz_localize(None).to_pydatetime()
    except Exception:
        try:
            return datetime.fromisoformat(ts)
        except Exception:
            return None

def map_api_record(record: dict) -> dict:
    """
    Map a single API record to the analytics-ready schema.
    Adjust keys according to the API response.
    """
    asset_id = record.get("id") or record.get("assetId") or record.get("uuid")
    last_updated = parse_timestamp(record.get("updated_at") or record.get("last_modified"))
    last_viewed = parse_timestamp(record.get("last_viewed"))
    last_refresh = parse_timestamp(record.get("last_refresh"))
    views_30d = record.get("views_last_30d") or record.get("views_30d") or 0
    refresh_status = record.get("refresh_status") or record.get("last_refresh_status") or "unknown"

    # derive status
    status = "active"
    if last_viewed is None:
        status = "stale"
    if refresh_status and refresh_status.lower() == "failed":
        status = "failed_refresh"

    return {
        "asset_id": str(asset_id),
        "asset_name": record.get("name") or record.get("title"),
        "owner": record.get("owner") or record.get("owner_email"),
        "last_updated": last_updated,
        "last_viewed": last_viewed,
        "views_last_30d": int(views_30d) if views_30d is not None else 0,
        "last_refresh": last_refresh,
        "refresh_status": refresh_status,
        "status": status,
    }

def records_to_dataframe(records: list) -> "pd.DataFrame":
    mapped = [map_api_record(r) for r in records]
    df = pd.DataFrame(mapped)
    # ensure timestamp columns are datetimes
    for col in ["last_updated", "last_viewed", "last_refresh"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    df["last_synced_at"] = pd.Timestamp.utcnow()
    return df
