import logging
import requests
from requests.exceptions import RequestException
from retrying import retry
from .config import settings
from .transform import records_to_dataframe
from .storage import get_engine, ensure_tables, upsert_dataframe

logger = logging.getLogger("prima_bi_metadata")
logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
                    format="%(asctime)s %(levelname)s %(message)s")

API_PAGE_SIZE = 100

@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def fetch_page(url, headers, params=None):
    params = params or {}
    resp = requests.get(url, headers=headers, params=params, timeout=10)
    resp.raise_for_status()
    # assume JSON response
    return resp.json()

def fetch_all_metadata():
    base = (settings.BI_API_BASE or "").rstrip("/")
    if not base:
        raise ValueError("BI_API_BASE non impostato in .env")

    # Se BI_API_BASE punta direttamente a un file .json, usalo così com'è
    if base.lower().endswith(".json"):
        endpoint = base
        use_pagination = False
    else:
        endpoint = f"{base}/metadata"
        use_pagination = True

    headers = {"Authorization": f"Bearer {settings.BI_API_KEY}"} if settings.BI_API_KEY else {}
    all_records = []

    if not use_pagination:
        logger.info("Fetching single JSON file %s", endpoint)
        try:
            data = fetch_page(endpoint, headers, params={})
        except RequestException as e:
            logger.exception("Error fetching JSON file: %s", e)
            return []
        records = data.get("items") or data.get("data") or data
        if records:
            all_records.extend(records if isinstance(records, list) else [records])
    else:
        page = 1
        while True:
            params = {"page": page, "page_size": API_PAGE_SIZE}
            logger.info("Fetching page %s from %s", page, endpoint)
            try:
                data = fetch_page(endpoint, headers, params=params)
            except RequestException as e:
                logger.exception("Network/API error while fetching page %s: %s", page, e)
                break
            records = data.get("items") or data.get("data") or data
            if not records:
                break
            all_records.extend(records if isinstance(records, list) else [records])
            # stop conditions
            if isinstance(data, dict) and (data.get("next") is None and data.get("has_more") is False):
                break
            if isinstance(records, list) and len(records) < API_PAGE_SIZE:
                break
            page += 1

    logger.info("Fetched %d records", len(all_records))
    return all_records

def run():
    logger.info("Run started")
    try:
        records = fetch_all_metadata()
        if not records:
            logger.info("No records fetched; exiting")
            return
        df = records_to_dataframe(records)
        engine = get_engine()
        ensure_tables(engine)
        count = upsert_dataframe(engine, df)
        logger.info("Upserted %d records", count)
    except Exception as e:
        logger.exception("Unexpected error: %s", e)
    finally:
        logger.info("Run finished")

if __name__ == "__main__":
    run()
