import sqlite3
import pandas as pd
from sqlalchemy import create_engine
from prima_bi_metadata.storage import upsert_dataframe

def test_upsert_inserts_rows(tmp_path):
    db_file = tmp_path / "metadata.db"
    engine = create_engine(f"sqlite:///{db_file}")
    # crea la tabella
    with engine.begin() as conn:
        conn.execute("""
        CREATE TABLE metadata (
          asset_id VARCHAR NOT NULL,
          asset_name VARCHAR,
          owner VARCHAR,
          last_updated DATETIME,
          last_viewed DATETIME,
          views_last_30d INTEGER,
          last_refresh DATETIME,
          refresh_status VARCHAR,
          status VARCHAR,
          last_synced_at DATETIME,
          PRIMARY KEY (asset_id)
        )
        """)
    # prepara un DataFrame di esempio
    df = pd.DataFrame([
        {"asset_id": "t-1", "asset_name": "Test", "owner": "me", "views_last_30d": 5}
    ])
    count = upsert_dataframe(engine, df)
    assert count == 1
    # verifica riga nel DB
    with engine.connect() as conn:
        res = conn.execute("SELECT COUNT(*) FROM metadata").scalar()
    assert res == 1
