import logging
import os
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


class OedsDataIO:
    """Dashboard-oriented loader for DB-backed datasets."""

    def __init__(self, db_mapping: dict | None = None, schema_name: str = "public") -> None:
        self.db_mapping = db_mapping or {}
        self.db_defaults = self.db_mapping.get("defaults", {})
        self.table_map = self.db_mapping.get("tables", {})
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

    @staticmethod
    def _clean_table_name(table_name: str) -> str:
        return table_name.lower().replace("-", "_").replace(" ", "_")[:63]

    def _resolve_table_spec(self, table_name: str, bidding_zone: str | None = None):
        clean_logical = self._clean_table_name(table_name)
        raw_spec = self.table_map.get(clean_logical, {})
        
        # If we have a bidding zone, construct the zone-specific table name
        if bidding_zone and raw_spec.get("table"):
            base_table = raw_spec.get("table")
            
            # Since all templates are now formatted as "BZ_table_name_suffix", 
            # we unconditionally split on the first underscore.
            parts = base_table.split("_", 1)  
            if len(parts) == 2:
                suffix = parts[1]  # e.g., "import_comm_flow_total_netted_per_type"
                table = f"{bidding_zone}_{suffix}"
            else:
                table = base_table
        else:
            table = raw_spec.get("table", clean_logical)
        
        return {
            "logical_table": clean_logical,
            "physical_table": table,
            "timestamp_column": raw_spec.get("timestamp_column", self.db_defaults.get("timestamp_column", "index")),
            "zone_column": raw_spec.get("zone_column", self.db_defaults.get("zone_column", "bidding_zone")),
            "drop_columns": raw_spec.get("drop_columns", []),
            "rename_columns": raw_spec.get("rename_columns", {}),
        }

    def _load_from_db(self, table_name: str, start: pd.Timestamp, end: pd.Timestamp, bz: str | None = None):
        if self.engine is None:
            return None

        spec = self._resolve_table_spec(table_name, bidding_zone=bz)
        physical_table = spec["physical_table"]
        timestamp_column = spec["timestamp_column"]
        zone_column = spec["zone_column"]

        print(f"🚀 DEBUG: App requested logical '{table_name}' for zone '{bz}'. Querying physical table: '{physical_table}'")

        try:
            # 1. Peek at the table columns (LIMIT 0 takes almost zero milliseconds)
            col_check_query = f'SELECT * FROM "{self.schema_name}"."{physical_table}" LIMIT 0'
            temp_df = pd.read_sql(text(col_check_query), self.engine)
            
            # 2. If "index" doesn't exist, assume the first column is the time column
            if len(temp_df.columns) > 0 and timestamp_column not in temp_df.columns:
                actual_time_col = temp_df.columns[0]
                print(f"⚠️ DEBUG: Column '{timestamp_column}' not found. Auto-switching to first column: '{actual_time_col}'")
                timestamp_column = actual_time_col

            start_str = start.strftime("%Y-%m-%d %H:%M:%S%z")
            end_str = end.strftime("%Y-%m-%d %H:%M:%S%z")

            # 3. Build the real query with the correct timestamp column
            query = (
                f'SELECT * FROM "{self.schema_name}"."{physical_table}" '
                f'WHERE "{timestamp_column}" >= \'{start_str}\' AND "{timestamp_column}" <= \'{end_str}\''
            )
            
            #if bz:
            #    query += f" AND {zone_column} = '{bz}'"

            df = pd.read_sql(text(query), self.engine)
            
            if df.empty:
                return None

            # 4. Safely set the index to our discovered time column
            index_col = timestamp_column if timestamp_column in df.columns else str(df.columns[0])
            df.set_index(index_col, inplace=True)
            df.index = pd.to_datetime(df.index, utc=True)
            df.index.name = None

            drop_columns = list(spec["drop_columns"])
            if bz and zone_column in df.columns and zone_column not in drop_columns:
                drop_columns.append(zone_column)
            existing_drop_cols = [c for c in drop_columns if c in df.columns]
            if existing_drop_cols:
                df = df.drop(columns=existing_drop_cols)

            rename_columns = spec["rename_columns"]
            if rename_columns:
                df = df.rename(columns=rename_columns)

            df.dropna(axis=1, how="all", inplace=True)
            return df
            
        except Exception as exc:
            logger.warning(
                "DB load failed for logical table '%s' (physical '%s', bz=%s): %s",
                spec["logical_table"],
                physical_table,
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
            # First, check what schemas exist
            schema_query = "SELECT schema_name FROM information_schema.schemata"
            schema_df = pd.read_sql(text(schema_query), self.engine)
            schemas = schema_df['schema_name'].tolist()
            
            # Then check tables in our target schema
            query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.schema_name}'"
            df = pd.read_sql(text(query), self.engine)
            tables = df['table_name'].tolist()
            
            # Also check all tables in all schemas for debugging
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
