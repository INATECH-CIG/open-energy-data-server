from io import StringIO
import os
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2 import sql
import time

env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

def get_flow_table_name(bz: str, flow_type: str, dayahead: bool, raw: bool):
    """returns the timescale table name for a given bidding zone. flowtype and dayahead flag"""
    return f"{bz}{'_raw' if raw else ''}_{flow_type}_flows{'_dayahead' if dayahead else ''}"

def get_connection(retries: int = 5):
    conn_params = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": "open-data-17",
        "port": "5432"
    }
    for trial in range(retries):
        try:
            conn = psycopg2.connect(**conn_params)
            return conn
        except psycopg2.OperationalError as e:
            print(f"Timescale Connection attempt {trial} failed: {e}")
            if trial < retries:
                print(f"Retrying in 3 seconds")
                time.sleep(3)
            else:
                raise Exception(f"Could not connect to the database after {retries} attempts.") from e

def ensure_schema(schemaname, readonly_user,  cur, conn):

    cur.execute(
        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}")
        .format(sql.Identifier(schemaname))
    )
    conn.commit()

    cur.execute(
        sql.SQL("GRANT USAGE ON SCHEMA {} TO {}")
        .format(
            sql.Identifier(schemaname),
            sql.Identifier(readonly_user)
        )
    )

    cur.execute(
        sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT ON TABLES TO {}")
        .format(
            sql.Identifier(schemaname),
            sql.Identifier(readonly_user)
        )
    )


def ensure_table(tablename, schemaname, df, cur,conn):

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

    create_sql = sql.SQL(
        "CREATE TABLE IF NOT EXISTS {}.{}({})"
    ).format(
        sql.Identifier(schemaname),
        sql.Identifier(tablename),
        sql.SQL(', ').join(sql.SQL(col_def) for col_def in col_defs)
    )
    sql.SQL(', ').join(sql.SQL(col_def) for col_def in col_defs)

    cur.execute(create_sql)
    conn.commit()

    full_table = f'{schemaname}."{tablename}"'
    hypertable_sql = "SELECT create_hypertable(%s, 'time', if_not_exists => TRUE);"
    cur.execute(hypertable_sql, (full_table,))
    conn.commit()

def df_to_timescale(df, tablename, schema_name ='public', config = None):
    """
    Writes a dataframe into a timescale db table
    """
    conn = get_connection()
    cur = conn.cursor()

    df = df.reset_index().rename(columns={"index": "time"})

    # Route metadata structures based on whether the data is raw extraction or downstream analysis
    is_result_table = tablename.startswith(("analysis_", "tracing_", "pool_", "annual_", "processed_"))

    if is_result_table:
        date_val = getattr(config, 'analysis_source_date', pd.Timestamp.utcnow().strftime('%Y-%m-%d'))
        df["source_download_date"] = date_val
        meta_cols = ["gap_filling_method", "bidding_zone", "source_download_date"]
    else:
        df["download_timestamp"] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
        meta_cols = ["gap_filling_method", "bidding_zone", "download_timestamp"]

    # Enforce column order to maintain tabular consistency
    data_cols = [c for c in df.columns if c not in meta_cols]
    present_meta = [c for c in meta_cols if c in df.columns]
    df = df[data_cols + present_meta]

    ensure_schema(schema_name, 'readonly', cur, conn)
    ensure_table(tablename, schema_name, df, cur, conn)

    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        sql.SQL("COPY {}.{} FROM STDIN WITH (FORMAT CSV)")
        .format(
            sql.Identifier(schema_name),
            sql.Identifier(tablename)
        ),
        buffer
    )

    conn.commit()
    cur.close()