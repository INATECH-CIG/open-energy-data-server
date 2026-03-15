from io import StringIO
import os
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
import psycopg2
import time

env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

def get_flow_table_name(bz: str, flow_type: str, dayahead: bool, raw: bool):
    """returns the timescale table name for a given bidding zone. flowtype and dayahead flag"""
    return f"{bz}{'_raw' if raw else ''}_{flow_type}_flows{'_dayahead' if dayahead else ''}"

def get_connection(retries: int = 5):
    try:
        conn_params = {
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": "open-data-17",
            "port":  "5432"
        }
        conn = psycopg2.connect(**conn_params)
        return conn
    except psycopg2.OperationalError as e:
        print('psycopg2.OperationalError: Retry connection to DB')
        print(str(e))
        time.sleep(3)
        if retries > 0:
            return get_connection(retries - 1)

def ensure_table(tablename, df):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = %s
        )
    """, (tablename.lower(),))
    exists = cur.fetchone()[0]

    if not exists:
        col_defs = []
        for col, dtype in zip(df.columns, df.dtypes):
            if pd.api.types.is_integer_dtype(dtype):
                sql_type = "BIGINT"
            elif pd.api.types.is_float_dtype(dtype):
                sql_type = "DOUBLE PRECISION"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                sql_type = "TIMESTAMPTZ"
            else:
                sql_type = "TEXT"
            col_defs.append(f'"{col}" {sql_type}')

        create_sql = f"CREATE TABLE {tablename}({', '.join(col_defs)});"
        cur.execute(create_sql)

        hypertable_sql = f"SELECT create_hypertable('{tablename}', 'time', if_not_exists => TRUE);"
        cur.execute(hypertable_sql)
        conn.commit()

    cur.close()
    conn.close()
    return tablename


def df_to_timescale(df, tablename):
    """
    Writes a dataframe into a timescale db table
    """
    conn = get_connection()
    cur = conn.cursor()

    df = df.reset_index().rename(columns={"index": "time"})

    ensure_table(tablename, df)

    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        f"COPY {tablename} FROM STDIN WITH (FORMAT CSV)",
        buffer
    )

    conn.commit()
    cur.close()