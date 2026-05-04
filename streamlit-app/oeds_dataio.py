import logging
import os
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


class OedsDataIO:
    """Dashboard-oriented loader for DB-backed datasets."""

    def __init__(self, schema_name: str = "public") -> None:
        self.schema_name = schema_name
        self.engine = self._build_engine()

    def _build_engine(self):
        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        db_name = os.getenv("POSTGRES_DB")
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")

        if not all([user, password, db_name]):
            logger.warning("DB credentials are incomplete; dashboard not retrieving data.")
            return None

        uri = f"postgresql://{user}:{quote_plus(password)}@{host}:{port}/{db_name}"
        print(f"Connecting to database: {uri}")
        return create_engine(uri, pool_pre_ping=True)

    def _load_from_db(self, table_name: str, start: pd.Timestamp, end: pd.Timestamp, bz: str | None = None):
        if self.engine is None:
            return None

        print(f"🚀 DEBUG: Requesting table '{table_name}' for zone '{bz}'...")

        try:
            # 1. Peek at the table columns (LIMIT 0 is extremely fast)
            col_check_query = f'SELECT * FROM "{self.schema_name}"."{table_name}" LIMIT 0'
            temp_df = pd.read_sql(text(col_check_query), self.engine)
            
            if temp_df.empty and len(temp_df.columns) == 0:
                return None

            # 2. Dynamically sniff the time column
            time_col = None
            for col in ["time", "timestamp", "index"]:
                if col in temp_df.columns:
                    time_col = col
                    break
            if not time_col: 
                time_col = temp_df.columns[0]
                print(f"⚠️ DEBUG: Time column not explicitly found. Defaulting to first column: '{time_col}'")

            # 3. Dynamically sniff the bidding zone column
            zone_col = "bidding_zone" if "bidding_zone" in temp_df.columns else None

            start_str = start.strftime("%Y-%m-%d %H:%M:%S%z")
            end_str = end.strftime("%Y-%m-%d %H:%M:%S%z")

            # 4. Build the core query
            query = (
                f'SELECT * FROM "{self.schema_name}"."{table_name}" '
                f'WHERE "{time_col}" >= \'{start_str}\' AND "{time_col}" <= \'{end_str}\''
            )
            
            if bz and zone_col:
                query += f" AND \"{zone_col}\" = '{bz}'"

            df = pd.read_sql(text(query), self.engine)
            
            if df.empty:
                return None

            # 5. Format the DataFrame cleanly for the App
            df.set_index(time_col, inplace=True)
            df.index = pd.to_datetime(df.index, utc=True)
            df.index.name = None

            # Drop the bidding_zone column to keep data pure for chart matrixes
            if zone_col and zone_col in df.columns:
                df = df.drop(columns=[zone_col])

            df.dropna(axis=1, how="all", inplace=True)
            return df
            
        except Exception as exc:
            logger.warning(
                "DB load failed for table '%s' (bz=%s): %s",
                table_name,
                bz,
                exc,
            )
            return None

    def load(
        self,
        table_name: str,
        start: pd.Timestamp,
        end: pd.Timestamp,
        bz: str | None = None,
    ):
        return self._load_from_db(table_name, start, end, bz=bz)

    def list_tables(self) -> list[str]:
        """List all tables in the schema."""
        if self.engine is None:
            return []
        
        try:
            schema_query = "SELECT schema_name FROM information_schema.schemata"
            schema_df = pd.read_sql(text(schema_query), self.engine)
            schemas = schema_df['schema_name'].tolist()
            
            query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.schema_name}'"
            df = pd.read_sql(text(query), self.engine)
            tables = df['table_name'].tolist()
            
            all_tables_query = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')"
            all_tables_df = pd.read_sql(text(all_tables_query), self.engine)
            
            return {
                'schemas': schemas,
                'target_schema_tables': tables,
                'all_tables': all_tables_df.to_dict('records')
            }
        except Exception as exc:
            logger.warning(f"Failed to list tables: {exc}")
            return []

    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        """Get schema information for a specific table."""
        if self.engine is None:
            return pd.DataFrame()
        
        try:
            query = f"""
            SELECT column_name, data_type, is_nullable, column_default 
            FROM information_schema.columns 
            WHERE table_schema = '{self.schema_name}' AND table_name = '{table_name}'
            ORDER BY ordinal_position
            """
            return pd.read_sql(text(query), self.engine)
        except Exception as exc:
            logger.warning(f"Failed to get schema for {table_name}: {exc}")
            return pd.DataFrame()

    def preview_table(self, table_name: str, limit: int = 10) -> pd.DataFrame:
        """Preview first few rows of a table."""
        if self.engine is None:
            return pd.DataFrame()
        
        try:
            query = f'SELECT * FROM "{self.schema_name}"."{table_name}" LIMIT {limit}'
            return pd.read_sql(text(query), self.engine)
        except Exception as exc:
            logger.warning(f"Failed to preview {table_name}: {exc}")
            return pd.DataFrame()

    def get_table_row_count(self, table_name: str) -> int:
        """Get total row count for a table."""
        if self.engine is None:
            return 0
        
        try:
            query = f'SELECT COUNT(*) as count FROM "{self.schema_name}"."{table_name}"'
            result = pd.read_sql(text(query), self.engine)
            return result['count'].iloc[0]
        except Exception as exc:
            logger.warning(f"Failed to get row count for {table_name}: {exc}")
            return 0