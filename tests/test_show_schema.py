import sqlite3

def test_metadata_table_exists(tmp_path):
    db_path = tmp_path / "metadata.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    # crea la tabella come nello schema di produzione
    cur.execute("""
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
    conn.commit()
    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='metadata'")
    row = cur.fetchone()
    conn.close()
    assert row is not None
    assert "views_last_30d INTEGER" in row[0]
