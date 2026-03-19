from sqlalchemy import create_engine, Table, Column, String, Integer, DateTime, MetaData, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
import pandas as pd
from typing import Optional
from .config import settings
from sqlalchemy import text

metadata = MetaData()

metadata_table = Table(
    "metadata",
    metadata,
    Column("asset_id", String, primary_key=True),
    Column("asset_name", String),
    Column("owner", String),
    Column("last_updated", DateTime),
    Column("last_viewed", DateTime),
    Column("views_last_30d", Integer),
    Column("last_refresh", DateTime),
    Column("refresh_status", String),
    Column("status", String),
    Column("last_synced_at", DateTime),
)

def get_engine(db_url: Optional[str] = None):
    db_url = db_url or settings.DB_URL
    engine = create_engine(db_url, future=True)
    return engine

def ensure_tables(engine):
    metadata.create_all(engine)

def upsert_dataframe(engine, df):
    """
    Upsert a DataFrame into the metadata table.
    - Normalizza NaN -> None
    - Converte Timestamp/NaT in stringhe ISO (o None)
    - Converte views_last_30d in int o None
    - Esegue bulk upsert usando una lista di mapping
    Restituisce il numero di record processati.
    """
    import pandas as pd
    from sqlalchemy import text
    from typing import Optional

    if df is None or df.empty:
        return 0

    # Normalizza NaN -> None
    df = df.where(pd.notnull(df), None)

    # Colonne attese (adatta se la tua tabella ha nomi diversi)
    expected_cols = [
        "asset_id", "asset_name", "owner", "last_updated", "last_viewed",
        "views_last_30d", "last_refresh", "refresh_status", "status", "last_synced_at"
    ]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    # Helper: converti Timestamp/NaT/Datetime in string ISO o None
    def _to_iso(v: Optional[object]) -> Optional[str]:
        if v is None:
            return None
        try:
            # pandas.Timestamp
            if hasattr(v, "isoformat"):
                # pd.Timestamp.isoformat() funziona anche con tz-aware
                return v.isoformat()
            # datetime.datetime
            if hasattr(v, "strftime"):
                return v.isoformat()
        except Exception:
            pass
        return None

    # Normalizza colonne datetime
    for dt_col in ("last_updated", "last_viewed", "last_refresh", "last_synced_at"):
        if dt_col in df.columns:
            df[dt_col] = df[dt_col].apply(_to_iso)

    # Normalizza views_last_30d: int o None
    if "views_last_30d" in df.columns:
        def _to_int_or_none(v):
            if v is None:
                return None
            try:
                return int(v)
            except (ValueError, TypeError, OverflowError):
                return None
        df["views_last_30d"] = df["views_last_30d"].apply(_to_int_or_none)

    # Prepara la query di upsert (adatta se la tabella ha schema diverso)
    stmt = text("""
    INSERT INTO metadata (
      asset_id, asset_name, owner, last_updated, last_viewed,
      views_last_30d, last_refresh, refresh_status, status, last_synced_at
    ) VALUES (
      :asset_id, :asset_name, :owner, :last_updated, :last_viewed,
      :views_last_30d, :last_refresh, :refresh_status, :status, :last_synced_at
    )
    ON CONFLICT (asset_id) DO UPDATE SET
      asset_name = excluded.asset_name,
      owner = excluded.owner,
      last_updated = excluded.last_updated,
      last_viewed = excluded.last_viewed,
      views_last_30d = excluded.views_last_30d,
      last_refresh = excluded.last_refresh,
      refresh_status = excluded.refresh_status,
      status = excluded.status,
      last_synced_at = excluded.last_synced_at
    """)

    # Prepara i record come lista di mapping con solo le colonne attese (ordine stabile)
    records = []
    for row in df[expected_cols].to_dict(orient="records"):
        # row è già composto da tipi Python (str, int, None) grazie alle trasformazioni sopra
        records.append(row)

    if not records:
        return 0

    conn = engine.connect()
    try:
        with conn.begin():
            # Bulk execute: passa la lista di dict come multiparams
            conn.execute(stmt, records)
        return len(records)
    finally:
        conn.close()
