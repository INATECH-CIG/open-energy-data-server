import time
import pandas as pd
import numpy as np
import sys
import logging
from pathlib import Path
from typing import Dict, Optional, List, Tuple, Any, Callable, Union
import os

logger = logging.getLogger(__name__)


class DataIO:
    """
    Centralized Data Input/Output Handler.
    Orchestrates dual-writing to local flat CSV files and a configured database instance.
    """

    def __init__(self, config: Any) -> None:
        self.save_db = getattr(config, 'save_db', False)
        self.load_source = getattr(config, 'load_source', 'csv')

        if self.save_db or self.load_source == 'db':
            pass
        else:
            self.engine = None
            logger.info("[IO] Running in CSV-only mode. No database engine initialized.")

    def save(self, df: Optional[Union[pd.DataFrame, pd.Series]], filepath: Path, table_name: str, config: Any,
             bz: Optional[str] = None) -> None:
        """Persists structural arrays to defined storage mediums, handling schema evolution."""
        if df is None or df.empty: return

        df_out = df.to_frame() if isinstance(df, pd.Series) else df.copy()

        if bz is not None:
            df_out["bidding_zone"] = bz

        # Route metadata structures based on whether the data is raw extraction or downstream analysis
        is_result_table = table_name.startswith(("analysis_", "tracing_", "pool_", "annual_", "processed_"))

        if is_result_table:
            date_val = getattr(config, 'analysis_source_date', pd.Timestamp.utcnow().strftime('%Y-%m-%d'))
            df_out["source_download_date"] = date_val
            meta_cols = ["gap_filling_method", "bidding_zone", "source_download_date"]
        else:
            df_out["download_timestamp"] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
            meta_cols = ["gap_filling_method", "bidding_zone", "download_timestamp"]

        # Enforce column order to maintain tabular consistency
        data_cols = [c for c in df_out.columns if c not in meta_cols]
        present_meta = [c for c in meta_cols if c in df_out.columns]
        df_out = df_out[data_cols + present_meta]

        # 1. Execute local flat-file persistence
        if getattr(config, 'save_csv', True):
            filepath.parent.mkdir(parents=True, exist_ok=True)
            df_out.to_csv(filepath)

        # 2. Execute relational database persistence
        if getattr(config, 'save_db', True):
            #clean_table = table_name.lower().replace("-", "_").replace(" ", "_")[:63]

            if bz is not None:
                if isinstance(df.index, pd.DatetimeIndex):
                    min_time = df.index.min().strftime('%Y-%m-%d %H:%M:%S%z')
                    max_time = df.index.max().strftime('%Y-%m-%d %H:%M:%S%z')
                    index_col = df.index.name or 'index'

                    delete_query = text(f"""
                        DELETE FROM {clean_table}
                        WHERE bidding_zone = '{bz}'
                        AND "{index_col}" >= '{min_time}'
                        AND "{index_col}" <= '{max_time}'
                    """)
                else:
                    delete_query = text(f"""
                        DELETE FROM {clean_table}
                        WHERE bidding_zone = '{bz}'
                    """)

                with self.engine.begin() as conn:
                    try:
                        conn.execute(delete_query)
                    except Exception:
                        pass

                        # Evaluate and apply dynamic schema evolution
            try:
                inspector = inspect(self.engine)
                if inspector.has_table(clean_table):
                    existing_cols = [col['name'] for col in inspector.get_columns(clean_table)]
                    new_cols = [c for c in df_out.columns if c not in existing_cols]

                    if new_cols:
                        with self.engine.begin() as conn:
                            for c in new_cols:
                                col_type = "TEXT" if c in meta_cols else "DOUBLE PRECISION"
                                conn.execute(text(f'ALTER TABLE {clean_table} ADD COLUMN "{c}" {col_type}'))
            except Exception as e:
                logger.warning(f"[DB Schema Warning] Could not auto-evolve schema for {clean_table}: {e}")

            try:
                df_out.to_sql(clean_table, self.engine, if_exists="append")
            except Exception as e:
                logger.error(f"[DB Error] Failed to save {clean_table} to database: {e}")

    def load(self, filepath: Path, table_name: str, config: Any, bz: Optional[str] = None) -> Optional[pd.DataFrame]:
        """Retrieves stored datasets based on configured storage preference (CSV vs DB)."""
        source = getattr(config, 'load_source', 'csv')
        start_str = config.start.strftime('%Y-%m-%d %H:%M:%S%z')
        end_str = config.end.strftime('%Y-%m-%d %H:%M:%S%z')

        if source == 'db':
            clean_table = table_name.lower().replace("-", "_").replace(" ", "_")[:63]
            try:
                base_query = f'SELECT * FROM {clean_table} WHERE "index" >= \'{start_str}\' AND "index" <= \'{end_str}\''
                query = f"{base_query} AND bidding_zone = '{bz}'" if bz is not None else base_query

                df = pd.read_sql(text(query), self.engine)
                if df.empty:
                    raise ValueError(f"No data found in DB for {clean_table} (bz={bz})")

                index_col = str(df.columns[0])
                df.set_index(index_col, inplace=True)
                df.index = pd.to_datetime(df.index, utc=True)
                df.index.name = None

                if bz is not None and "bidding_zone" in df.columns:
                    df = df.drop(columns=["bidding_zone"])

                df.dropna(axis=1, how='all', inplace=True)
                return df

            except Exception as e:
                logger.warning(f"[DB Warning] Falling back to CSV for {clean_table} (bz={bz}). Reason: {e}")

        if filepath.exists():
            df = pd.read_csv(filepath, index_col=0)
            df.index = pd.to_datetime(df.index, utc=True)
            mask = (df.index >= config.start) & (df.index <= config.end)
            return df.loc[mask]

        return None


# ==========================================
# LOGGING UTILS
# ==========================================
class DualLogger:
    """Redirects output to both terminal and a log file."""
    def __init__(self, filepath, stream):
        self.terminal = stream
        self.log = open(filepath, 'a', encoding='utf-8')

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()

    def flush(self):
        self.terminal.flush()
        self.log.flush()

def _record_gap_method(df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, method: str, col_name: str = "ROW") -> None:
    """Appends the specified imputation methodology to the metadata audit trail for a given temporal range."""
    if "gap_filling_method" not in df.columns:
        df["gap_filling_method"] = "None"

    mask = (df.index >= start) & (df.index <= end)
    tagged_method = f"[{col_name}] {method}"

    none_mask = mask & (df["gap_filling_method"] == "None")
    df.loc[none_mask, "gap_filling_method"] = tagged_method

    exist_mask = mask & (df["gap_filling_method"] != "None")

    def append_if_missing(current: str) -> str:
        return current if tagged_method in str(current) else f"{current}, {tagged_method}"

    df.loc[exist_mask, "gap_filling_method"] = df.loc[exist_mask, "gap_filling_method"].apply(append_if_missing)

def _merge_gap_methods(df_target: pd.DataFrame, df_source: pd.DataFrame) -> None:
    """Consolidates metadata strings when combining parallel datasets to maintain a unified audit trail."""
    if "gap_filling_method" not in df_source.columns: return
    if "gap_filling_method" not in df_target.columns:
        df_target["gap_filling_method"] = "None"

    valid_methods = df_source.loc[(df_source["gap_filling_method"] != "None") & df_source["gap_filling_method"].notna(), "gap_filling_method"]
    
    for t, method in valid_methods.items():
        if t in df_target.index:
            curr = df_target.at[t, "gap_filling_method"]
            if curr == "None":
                df_target.at[t, "gap_filling_method"] = method
            elif method not in str(curr):
                df_target.at[t, "gap_filling_method"] = f"{curr}, {method}"

# ==========================================
# API UTILS
# ==========================================
def safe_query(func, max_retries=3, delay=2, context=None, **kwargs):
    """Executes API calls with retries and descriptive error logging."""
    for attempt in range(max_retries):
        try:
            return func(**kwargs)
        except Exception as e:
            msg = f"[Attempt {attempt + 1}/{max_retries}] Failed"
            if context: msg += f" for {context}"
            msg += f": {str(e)}"
            print(msg)
            
            if "No matching data found" in str(e): return None

            if attempt < max_retries - 1:
                time.sleep(delay)
            else:
                print(f"Skipping {context if context else 'query'} after max retries.")
                return None
    return None

# ==========================================
# GAP FILLING ENGINE (Qussous & Grether)
# ==========================================

def default_rules(series: pd.Series, gaps: pd.DataFrame, inferred_freq: pd.Timedelta):
    # use zero as fallback and for negative values
    gaps["method"] = "ZERO"

    # use week before for larger gaps
    MAX_WEEK_BEFORE = pd.Timedelta(weeks=1)
    gaps.loc[
        (gaps["type"] == "nan")
        & (gaps["duration"] * inferred_freq <= MAX_WEEK_BEFORE)
        & (
            gaps["start"] - series.index[0] >= MAX_WEEK_BEFORE
        ),  # ensure there exists a week before to fill with
        "method",
    ] = "WEEK_BEFORE"

    # use linear interpolation for small gaps
    MAX_LINEAR = pd.Timedelta(hours=3)
    gaps.loc[
        (gaps["type"] == "nan")
        & (gaps["duration"] * inferred_freq <= MAX_LINEAR)
        & (gaps["start"] > series.index[0])
        & (gaps["end"] < series.index[-1]),  # ensure we are not on the edge
        "method",
    ] = "LINEAR"

    # use forward fill for edge gap at the end
    gaps.loc[
        (gaps["type"] == "nan")
        & (gaps["duration"] * inferred_freq <= MAX_LINEAR)
        & (gaps["start"] > series.index[0])
        & (gaps["end"] == series.index[-1]),
        "method",
    ] = "FORWARD_FILL"

    # use backward fill for edge gap in the beginning
    gaps.loc[
        (gaps["type"] == "nan")
        & (gaps["duration"] * inferred_freq <= MAX_LINEAR)
        & (gaps["start"] == series.index[0])
        & (gaps["end"] < series.index[-1]),
        "method",
    ] = "BACKWARD_FILL"

def fill_gaps_series(series: pd.Series, gaps: pd.DataFrame):
    # add output columns
    gaps["success"] = False
    gaps["filled_values"] = 0
    gaps["filled_quantity"] = 0.0

    for i, gap in gaps.iterrows():
        start, end = gap["start"], gap["end"]
        duration = gap["duration"]
        method = gap["method"]

        if method == "ZERO":
            series.loc[start:end] = 0

        elif method == "LINEAR":
            pos_start = series.index.get_loc(start)
            pos_precursor = pos_start - 1
            pos_successor = pos_start + duration
            # Interpolate
            series.loc[start:end] = np.linspace(
                series.iloc[pos_precursor], series.iloc[pos_successor], duration + 2
            )[1:-1]

        elif method == "FORWARD_FILL":
            pos_start = series.index.get_loc(start)
            series.loc[start:end] = series.iloc[pos_start - 1]

        elif method == "BACKWARD_FILL":
            pos_start = series.index.get_loc(start)
            series.loc[start:end] = series.iloc[pos_start + duration]

        elif method == "WEEK_BEFORE":
            one_week = pd.Timedelta(weeks=1)
            week_before_start = start - one_week
            week_before_end = end - one_week
            # Fill with data from week before
            series.loc[start:end] = series.loc[week_before_start:week_before_end].values

        # Validation
        filled_values = series.loc[start:end].count()
        filled_quantity = series.loc[start:end].sum()
        success = filled_values > 0

        gaps.loc[i, "success"] = success
        gaps.loc[i, "filled_values"] = filled_values
        gaps.loc[i, "filled_quantity"] = filled_quantity

    return series, gaps

def find_gaps_series(
    series: pd.Series,
    output_dict: dict = None,
    check_negatives: bool = False,
    allow_negatives: list = [],
    fill_gaps: bool = False,
    gap_filling_rules: callable = None,
):
    # Clean massive outliers
    series = series.where(series < 100000, np.nan)

    # Find NaNs
    is_nan = series.isna()
    gap_starts = is_nan & (~is_nan.shift(1, fill_value=False))
    gap_ends = is_nan & (~is_nan.shift(-1, fill_value=False))

    gaps = pd.DataFrame({"start": series[gap_starts].index, "end": series[gap_ends].index})
    
    gaps["duration"] = gaps.apply(
        lambda row: is_nan[row["start"] : row["end"]].sum(), 
        axis=1,
        result_type="reduce"
    ).astype("int")
    
    gaps["value"] = np.nan
    gaps["type"] = "nan"

    # Optional: Check Negatives
    if check_negatives and (str(series.name) not in allow_negatives):
        is_neg = series < 0
        neg_starts = is_neg & (~is_neg.shift(1, fill_value=False))
        neg_ends = is_neg & (~is_neg.shift(-1, fill_value=False))
        
        negs = pd.DataFrame({"start": series[neg_starts].index, "end": series[neg_ends].index})
        
        # --- FIX 2: ADDED result_type="reduce" ---
        negs["duration"] = negs.apply(
            lambda row: is_neg[row["start"] : row["end"]].sum(), 
            axis=1,
            result_type="reduce"
        ).astype("int")

        # --- FIX 3: ADDED result_type="reduce" ---
        negs["value"] = negs.apply(
            lambda row: series[row["start"] : row["end"]].sum(), 
            axis=1,
            result_type="reduce"
        )
        negs["type"] = "negative"
        
        gaps = pd.concat([gaps, negs]).sort_values(by="start").reset_index(drop=True)

    # Infer Frequency
    inferred_freq = pd.infer_freq(series.index[:3])
    if (inferred_freq is not None) and (len(inferred_freq) == 1):
        inferred_freq = "1" + inferred_freq
    inferred_freq = pd.to_timedelta(inferred_freq) if inferred_freq else pd.Timedelta(hours=1)

    gaps["method"] = "UNDEFINED"

    # Set rules
    if gap_filling_rules is not None:
        gap_filling_rules(series, gaps, inferred_freq)

    # Fill
    if fill_gaps:
        series, gaps = fill_gaps_series(series, gaps)

    if output_dict is not None:
        output_dict[series.name] = gaps

    return series

def find_gaps(
    df: pd.DataFrame,
    check_negatives: bool = False,
    allow_negatives: list = [],
    fill_gaps: bool = False,
    gap_filling_rules: callable = default_rules,
):
    output_dict = {}
    df = df.apply(
        find_gaps_series,
        axis=0,
        output_dict=output_dict,
        check_negatives=check_negatives,
        allow_negatives=allow_negatives,
        fill_gaps=fill_gaps,
        gap_filling_rules=gap_filling_rules,
    )
    return df, output_dict

def patch_gaps_with_dayahead(
    flow_df: pd.DataFrame,
    gap_dict: Dict[str, pd.DataFrame],
    bz: str,
    neighbour: str,
    config: Any, 
    min_gap_length: pd.Timedelta = pd.Timedelta(weeks=1)
) -> pd.DataFrame:
    """Leverages day-ahead commercial schedules as a physical proxy to impute extended missing flow blocks."""
    long_gaps: List[Tuple[str, pd.Timestamp, pd.Timestamp]] = []
    for col in [f"{bz}_{neighbour}", f"{neighbour}_{bz}"]:
        if col in gap_dict:
            for _, row in gap_dict[col].iterrows():
                if (row["end"] - row["start"]) > min_gap_length:
                    long_gaps.append((col, row["start"], row["end"]))

    if not long_gaps:
        return flow_df  # No long gaps, nothing to do

    # 2. Load Day-Ahead Data (Only if needed)
    dayahead_path = config.get_output_path("comm_flow_dayahead_bidding_zones") / f"{bz}_comm_flow_dayahead_bidding_zones.csv"
    
    if not dayahead_path.exists():
        print(f"   [Warning] Long gap detected for {bz}<->{neighbour}, but no Day-Ahead file found to patch it.")
        return flow_df

    try:
        da_df = pd.read_csv(dayahead_path, index_col=0)
        da_df.index = pd.to_datetime(da_df.index, utc=True)
    except Exception as e:
        print(f"   [Error] Failed to load DA file for {bz}: {e}")
        return flow_df

    patched_count = 0
    for col, start, end in long_gaps:
        if col in da_df.columns:
            replacement = da_df.loc[start:end, col]

            if not (replacement.empty or replacement.isna().all()):
                flow_df.loc[start:end, col] = replacement
                patched_count += 1
                _record_gap_method(flow_df, start, end, "DAYAHEAD_PROXY", col_name=col)

    if patched_count > 0:
        logger.info(f"   -> [Patch] Used {dayahead_path} to fill {patched_count} long-duration gaps for {bz}.")

    return flow_df

# ==========================================
# DATA PROCESSING WRAPPERS
# ==========================================

def fill_gaps_wrapper(df: pd.DataFrame,
                      gaps_dir,
                      prefix,
                      config=None,
                      bz=None,
                      flow_type=None,
                      dayahead=False) -> pd.DataFrame:
    """Orchestrates the detection, rule assignment, and execution of the gap-filling sequence."""
    if df.empty: return df
    
    if "gap_filling_method" not in df.columns:
        df["gap_filling_method"] = "None"

    _, gaps_dict = find_gaps(df, check_negatives=False, fill_gaps=False)

    if config and bz and (flow_type == "commercial") and (not dayahead):
        if hasattr(config, 'neighbours_map') and bz in config.neighbours_map:
            for neighbour in [n for n in config.neighbours_map[bz] if f"{bz}_{n}" in df.columns]:
                df = patch_gaps_with_dayahead(df, gaps_dict, bz, neighbour, config)

    df_filled, new_gaps_dict = find_gaps(df, check_negatives=False, fill_gaps=True, gap_filling_rules=default_rules)

    for col_name, gap_df in new_gaps_dict.items():
        if gap_df.empty: continue
        for _, row in gap_df.iterrows():
            if row.get("success", True):
                _record_gap_method(df_filled, row["start"], row["end"], row["method"], col_name=str(col_name))

    if gaps_dir:
        for key, gap_df in new_gaps_dict.items():
            file_path = gaps_dir / f"{prefix}_{str(key).replace('/', '_').replace(' ', '_')}_gaps.csv"
            if not gap_df.empty:
                gap_df.to_csv(file_path)
            else:
                if file_path.exists():
                    file_path.unlink()

    return df_filled

def correct_zero_values(df: pd.DataFrame, gaps_dir, bz, config):
    """
    Patches zero-values using data from +/- 1 week.
    - Generation: Checks if 'Total Generation' == 0.
    - Flows: Checks if the entire row (all columns) sum to 0.
    """
    if df.empty: return df

    # 1. Determine Zero Mask based on Data Type
    if "Total Generation" in df.columns:
        # Generation Logic
        zeros_mask = df["Total Generation"] == 0
    else:
        # Flow Logic: Check if row has no active flows (all numeric cols are 0)
        numeric_cols = df.select_dtypes(include=[np.number])
        zeros_mask = (numeric_cols != 0).sum(axis=1) == 0

    zeros_df = df[zeros_mask]

    # 2. Patch Data if Zeros Found
    if len(zeros_df) > 0:
        print(f"   -> [{bz}] Found {len(zeros_df)} zero-rows. Patching with +/- 1 week data...")
        one_week = pd.Timedelta(weeks=1)
        range_start = config.start

        for timestamp in zeros_df.index:
            # Default: Look back 1 week
            patch_time = timestamp - one_week
            
            # If 1 week back is before start date, look forward 1 week
            if patch_time < range_start:
                patch_time = timestamp + one_week
            
            # Apply patch if data exists
            if patch_time in df.index:
                df.loc[timestamp] = df.loc[patch_time]

        # 3. Save Log
        zeros_df.to_csv(gaps_dir / f"{bz}_zeros.csv")

    return df