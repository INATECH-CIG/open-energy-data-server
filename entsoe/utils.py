import time
import pandas as pd
import numpy as np
import sys
import os

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

def start_logging(log_file_path):
    """Sets up stdout/stderr redirection."""
    log_dir = os.path.dirname(log_file_path)
    if log_dir: os.makedirs(log_dir, exist_ok=True)

    if isinstance(sys.stdout, DualLogger): sys.stdout = sys.stdout.terminal
    if isinstance(sys.stderr, DualLogger): sys.stderr = sys.stderr.terminal

    sys.stdout = DualLogger(log_file_path, sys.stdout)
    sys.stderr = DualLogger(log_file_path, sys.stderr)
    
    print(f"--- Log Started: {pd.Timestamp.now()} ---")
    print(f"Saving logs to: {log_file_path}")

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

def patch_gaps_with_dayahead(flow_df, gap_dict, bz, neighbour, config, min_gap_length=pd.Timedelta(weeks=1)):
    """
    Patches gaps in commercial flow data using Day-Ahead (Scheduled) data 
    if the gap duration exceeds 'min_gap_length'.
    """
    # 1. Check if we have any long gaps to patch
    long_gaps = []
    
    # Check Outgoing Gaps (BZ -> Neighbor)
    col_out = f"{bz}_{neighbour}"
    if col_out in gap_dict:
        for _, row in gap_dict[col_out].iterrows():
            duration = row["end"] - row["start"]
            if duration > min_gap_length:
                long_gaps.append((col_out, row["start"], row["end"]))

    # Check Incoming Gaps (Neighbor -> BZ)
    col_in = f"{neighbour}_{bz}"
    if col_in in gap_dict:
        for _, row in gap_dict[col_in].iterrows():
            duration = row["end"] - row["start"]
            if duration > min_gap_length:
                long_gaps.append((col_in, row["start"], row["end"]))

    if not long_gaps:
        return flow_df  # No long gaps, nothing to do

    # 2. Load Day-Ahead Data (Only if needed)
    dayahead_path = config.get_output_path("comm_flow_dayahead_bidding_zones") / f"{bz}_commercial_flows_dayahead_bidding_zones.csv"
    
    if not dayahead_path.exists():
        print(f"   [Warning] Long gap detected for {bz}<->{neighbour}, but no Day-Ahead file found to patch it.")
        return flow_df

    try:
        da_df = pd.read_csv(dayahead_path, index_col=0)
        da_df.index = pd.to_datetime(da_df.index, utc=True)
    except Exception as e:
        print(f"   [Error] Failed to load DA file for {bz}: {e}")
        return flow_df

    # 3. Apply Patches
    patched_count = 0
    for col, start, end in long_gaps:
        if col in da_df.columns:
            # logging the replacement
            print(f"   [Patching] Replacing {col} with Day-Ahead values from {start} to {end}")
            
            # Ensure alignment
            replacement = da_df.loc[start:end, col]
            
            # If DA data is missing too, we can't patch
            if replacement.empty or replacement.isna().all():
                 print("      [Warning] Day-Ahead data also missing/empty for this period.")
                 continue
                 
            flow_df.loc[start:end, col] = replacement
            patched_count += 1
        else:
            print(f"      [Warning] Day-Ahead file missing column {col}.")

    if patched_count > 0:
        print(f"   -> Successfully patched {patched_count} long gaps using Day-Ahead data.")
        
    return flow_df

# ==========================================
# DATA PROCESSING WRAPPERS
# ==========================================

def fill_gaps_wrapper(df: pd.DataFrame, gaps_dir, prefix, config=None, bz=None, 
                      flow_type=None, dayahead=False):
    """
    Wrapper for find_gaps.
    1. Finds gaps.
    2. Patches large commercial flow gaps with Day-Ahead data (if config/bz provided).
    3. Statistically fills remaining gaps.
    4. Saves reports.
    """
    if df.empty: return df
    
    # 1. Find initial gaps (do not fill yet)
    _, gaps_dict = find_gaps(df, check_negatives=False, fill_gaps=False)

    # 2. Patch Large Gaps with Day-Ahead (Commercial Flows Only)            
    if config and bz and (flow_type == "commercial") and (not dayahead):
        
        if bz in config.neighbours_map:
            potential_neighbors = config.neighbours_map[bz]
            # Only process if the column 'BZ_Neighbor' actually exists in this DF
            actual_neighbors = [n for n in potential_neighbors if f"{bz}_{n}" in df.columns]
            
            for neighbour in actual_neighbors:
                df = patch_gaps_with_dayahead(df, gaps_dict, bz, neighbour, config)

    # 3. Statistically Fill Remaining Gaps
    df_filled, final_gaps = find_gaps(
        df, 
        check_negatives=False, 
        fill_gaps=True, 
        gap_filling_rules=default_rules
    )
    
    # 4. Save Reports for columns that were filled
    if gaps_dir:
        for key, gap_df in gaps_dict.items():
            if not gap_df.empty:
                safe_key = str(key).replace("/", "_").replace(" ", "_")
                gap_df.to_csv(gaps_dir / f"{prefix}_{safe_key}_gaps.csv")
    
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