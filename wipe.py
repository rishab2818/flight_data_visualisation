# wipe_db.py
from sqlalchemy import create_engine, text

url = "postgresql+psycopg2://flightuser:1234@127.0.0.1:5432/flightdb"
engine = create_engine(url, future=True)

with engine.begin() as conn:
    # Option C equivalent (truncate all public tables)
    conn.execute(text("""
    DO $$
    DECLARE stmt text;
    BEGIN
      SELECT string_agg(format('TRUNCATE TABLE %I.%I RESTART IDENTITY CASCADE', schemaname, tablename), '; ')
        INTO stmt
      FROM pg_tables
      WHERE schemaname = 'public';
      IF stmt IS NOT NULL THEN
        EXECUTE stmt;
      END IF;
    END $$;
    """))
