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

def ensure_schema(schemaname):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
                SELECT EXISTS (SELECT 1
                               FROM information_schema.schemata
                               WHERE schema_name = %s)
                """, (schemaname,))
    exists = cur.fetchone()[0]

    if not exists:
        cur.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}")
            .format(sql.Identifier(schemaname))
        )

    conn.commit()
    cur.close()
    conn.close()

def ensure_table(tablename, schemaname, df):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        )
    """, (schemaname.lower(), tablename.lower()))
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

        create_sql = sql.SQL("CREATE TABLE {}.{}({})").format(
            sql.Identifier(schemaname),
            sql.Identifier(tablename),
            sql.SQL(', ').join(sql.SQL(col_def) for col_def in col_defs)
        )
        cur.execute(create_sql)

        hypertable_sql = sql.SQL("SELECT create_hypertable({}.{}, 'time', if_not_exists => TRUE);").format(
            sql.Identifier(schemaname),
            sql.Identifier(tablename)
        )
        cur.execute(hypertable_sql)
        conn.commit()

    cur.close()
    conn.close()
    return


def df_to_timescale(df, tablename, schema =  'public'):
    """
    Writes a dataframe into a timescale db table
    """
    conn = get_connection()
    cur = conn.cursor()

    df = df.reset_index().rename(columns={"index": "time"})

    ensure_schema()
    ensure_table(tablename, df)

    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        sql.SQL("COPY {}.{} FROM STDIN WITH (FORMAT CSV)")
        .format(
            sql.Identifier(schema),
            sql.Identifier(tablename)
        ),
        buffer
    )

    conn.commit()
    cur.close()