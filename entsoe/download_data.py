"""
Project: European Electricity Exchange Analysis
Author: Tiernan Buckley
Year: 2026
License: Creative Commons Attribution 4.0 International (CC BY 4.0)
Source: https://github.com/INATECH-CIG/exchange_analysis

Description:
Connects to the ENTSO-E API to download, enforce structural symmetry on, 
and standardize raw generation, demand, and cross-border flow data.
"""

import pandas as pd
import requests
import logging
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Dict, Optional, Any
from entsoe import EntsoePandasClient
from config import PipelineConfig
from utils import safe_query, fill_gaps_wrapper, correct_zero_values, _merge_gap_methods
from postgres_utils import df_to_timescale, get_flow_table_name

logger = logging.getLogger(__name__)


TIMEOUT = 60
GB_GENERATION_TYPES = [
    "Biomass", "Fossil Gas", "Fossil Hard coal", "Fossil Oil", 
    "Hydro Pumped Storage", "Hydro Run-of-river and poundage", 
    "Nuclear", "Other", "Solar", "Wind Offshore", "Wind Onshore"
]

# ==========================================
# GENERATION & DEMAND
# ==========================================
def download_generation_demand(client: EntsoePandasClient, config: PipelineConfig) -> None:
    """
    Retrieves raw generation and demand data for configured target zones.
    Routes GB queries to the BMRS API and all others to the ENTSO-E client.
    """
    if not config.data_types["generation"]: return
    raw_dir = config.get_output_path("generation_demand_data_bidding_zones") / "raw"
    
    for bz in config.target_zones:
        logger.info(f"[Download] Gen/Demand for {bz}...")
        gen_df: Optional[pd.DataFrame] = None
        load_df: Optional[pd.DataFrame] = None

        if bz == "GB":
            try:
                logger.info(f"  -> Using BMRS API for GB")
                gen_df = download_GB_per_type_data(config.start, config.end)
                load_df = download_GB_demand_data(config.start, config.end)
            except Exception as e: 
                logger.error(f"[Error] Failed to download GB data: {e}", exc_info=config.debug_mode)
        else:
            gen_df = safe_query(client.query_generation, context=f"Generation {bz}", country_code=bz, start=config.start, end=config.end, nett=True)
            load_df = safe_query(client.query_load, context=f"Load {bz}", country_code=bz, start=config.start, end=config.end)

        if gen_df is not None:
            gen_df.to_csv(raw_dir / f"{bz}_raw_generation.csv")
            df_to_timescale(gen_df, f"{bz}_raw_generation", config.db_schema_name)
        if load_df is not None:
            load_df.to_csv(raw_dir / f"{bz}_raw_load.csv")
            df_to_timescale(gen_df, f"{bz}_raw_load", config.db_schema_name)

def process_generation_demand(config: PipelineConfig) -> Dict[str, pd.DataFrame]:
    """
    Cleans, resamples, and merges raw generation and load data.
    Enforces gap-filling heuristics and recalculates structural net exports.
    Operates in two phases to guarantee a globally synchronized data vintage.
    """
    raw_dir = config.get_output_path("generation_demand_data_bidding_zones") / "raw"
    out_dir = config.get_output_path("generation_demand_data_bidding_zones")
    gaps_dir = config.get_gaps_path("generation_demand_data_bidding_zones")
    
    gen_storage_dict: Dict[str, pd.DataFrame] = {}
    vintages = []

    # ========================================================
    # PHASE 1: PROCESS AND COLLECT
    # ========================================================
    # Ingest data, apply local transformations, and collect metadata strings
    for bz in config.zones:
        logger.info(f"[Process] Gen./Demand for {bz}...")
        gen_path = raw_dir / f"{bz}_raw_generation.csv"
        load_path = raw_dir / f"{bz}_raw_load.csv"
        
        gen_df = pd.read_csv(gen_path, index_col=0) if gen_path.exists() else None
        load_df = pd.read_csv(load_path, index_col=0) if load_path.exists() else None
        
        # Helper to extract the data vintage from either internal columns or OS file metadata
        def extract_vintage(df: Optional[pd.DataFrame], path: Path) -> None:
            if df is not None:
                if "download_timestamp" in df.columns:
                    vintages.append(str(df["download_timestamp"].iloc[0]).split()[0])
                elif "source_download_date" in df.columns:
                    vintages.append(str(df["source_download_date"].iloc[0]).split()[0])
                elif path.exists():
                    vintages.append(datetime.fromtimestamp(path.stat().st_mtime).strftime('%Y-%m-%d'))
                else:
                    vintages.append(pd.Timestamp.utcnow().strftime('%Y-%m-%d'))

        extract_vintage(gen_df, gen_path)
        extract_vintage(load_df, load_path)

        if gen_df is None and load_df is None:
            logger.warning(f"No raw generation/load data found for {bz}. Skipping.")
            continue

        meta_cols = ["download_timestamp", "bidding_zone", "gap_filling_method"]

        # 1. Process Generation Data
        if gen_df is not None:
            gen_df = gen_df.loc[:, ~gen_df.columns.duplicated()]
            
            # Isolate numerical features prior to resampling to avoid metadata corruption
            data_cols = [c for c in gen_df.columns if c not in meta_cols]
            gen_df[data_cols] = gen_df[data_cols].apply(pd.to_numeric, errors='coerce')
            gen_df = gen_df.resample("1h").mean(numeric_only=True)

            gen_df = fill_gaps_wrapper(gen_df, gaps_dir, f"{bz}_gen", config=config, bz=bz)

            # Handle storage components separately to calculate distinct charge/discharge profiles
            storage_cols = [c for c in ["Hydro Pumped Storage", "Energy storage"] if c in gen_df.columns]
            if storage_cols:
                storage_series = gen_df[storage_cols].fillna(0).sum(axis=1)
                gen_df = gen_df.drop(columns=storage_cols)
            else:
                storage_series = None

            num_cols = gen_df.select_dtypes(include=['number']).columns
            if "gap_filling_method" not in gen_df.columns:
                gen_df["gap_filling_method"] = "None"

            # Enforce non-negativity constraint on generation sources
            for col in num_cols:
                col_neg_mask = gen_df[col] < 0
                if col_neg_mask.any():
                    gen_df[col] = gen_df[col].clip(lower=0.0)
                    method_tag = f"[{col}] CLIPPED_NEGATIVE"
                    curr_methods = gen_df.loc[col_neg_mask, "gap_filling_method"]
                    gen_df.loc[col_neg_mask, "gap_filling_method"] = curr_methods.apply(
                        lambda x: method_tag if str(x) == "None"
                        else (x if method_tag in str(x) else f"{x}, {method_tag}")
                    )

            # Aggregate physical totals
            gen_df["Generation"] = gen_df[num_cols].sum(axis=1)
            gen_df["Storage Discharge"] = storage_series.clip(lower=0.0) if storage_series is not None else 0.0
            gen_df["Storage Charge"] = storage_series.clip(upper=0.0).abs() if storage_series is not None else 0.0

        # 2. Process Load Data
        if load_df is not None:
            data_cols = [c for c in load_df.columns if c not in meta_cols]
            load_df[data_cols] = load_df[data_cols].apply(pd.to_numeric, errors='coerce')
            load_df = load_df.resample("1h").mean(numeric_only=True)
            load_df = fill_gaps_wrapper(load_df, gaps_dir, f"{bz}_load", config=config, bz=bz)

        # 3. Merge and Balance
        if gen_df is not None:
            if load_df is not None:
                for k in ["Actual Load", "Load"]:
                    if k in load_df.columns: gen_df["Demand"] = load_df[k]
                
                _merge_gap_methods(gen_df, load_df)

                gen_df["Total Generation"] = gen_df["Generation"] + gen_df["Storage Discharge"]
                gen_df["Total Load"] = gen_df.get("Demand", 0) + gen_df["Storage Charge"]
            
            # Identify and patch systemic zeros
            gen_df = correct_zero_values(gen_df, gaps_dir, bz, config)

            # Recalculate Net Export strictly post-patching to preserve arithmetic integrity
            if "Total Generation" in gen_df.columns and "Total Load" in gen_df.columns:
                gen_df["Net Export"] = gen_df["Total Generation"] - gen_df["Total Load"]

            # Store in memory to await global synchronization
            gen_storage_dict[bz] = gen_df

    # ========================================================
    # PHASE 2: SYNCHRONIZE VINTAGE AND SAVE
    # ========================================================
    # Determines the latest temporal vintage across all processed zones and applies it globally
    if vintages:
        current_batch_latest = max(vintages)
        existing_global = getattr(config, "analysis_source_date", current_batch_latest)
        config.analysis_source_date = max(existing_global, current_batch_latest)

        unique_dates = set(vintages)
        if len(unique_dates) > 1:
            logger.warning(f"Data vintage mismatch detected in Gen/Load: {unique_dates}. Synchronized to {config.analysis_source_date}.")
        else:
            logger.info(f"[Metadata] Gen/Load data vintage synchronized to: {config.analysis_source_date}")

    # Commit synchronized dataframes to designated IO channels
    for bz, final_df in gen_storage_dict.items():
        io.save(final_df, out_dir / f"{bz}_generation_demand_data_bidding_zones.csv", "processed_generation", config, bz=bz)

    return gen_storage_dict

# ==========================================
# FLOWS
# ==========================================

def download_flows(client: EntsoePandasClient, config: PipelineConfig, flow_type: str = "commercial", dayahead: bool = False):
    """
    Downloads scheduled commercial or physical cross-border exchanges.
    Builds the bilateral combinations based on the defined neighbor map.
    """
    if flow_type == "commercial" and not config.data_types.get(f"flows_commercial{'_dayahead' if dayahead else '_total'}"): return
    if flow_type == "physical" and not config.data_types.get("flows_physical"): return

    folder = "physical_flow_data_bidding_zones" if flow_type == "physical" else f"comm_flow_{'dayahead' if dayahead else 'total'}_bidding_zones"
    raw_dir = config.get_output_path(folder) / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    for bz in config.target_zones:
        logger.info(f"[Download] {flow_type} flows for {bz} (Dayahead={dayahead})...")

        flow_df: Optional[pd.DataFrame] = None
        for n in [z for z in config.neighbours_map[bz] if z in config.zones]:
            if flow_type == "commercial":
                f_out = safe_query(client.query_scheduled_exchanges, context=f"{bz}->{n}", country_code_from=bz, country_code_to=n, start=config.start, end=config.end, dayahead=dayahead)
                f_in = safe_query(client.query_scheduled_exchanges, context=f"{n}->{bz}", country_code_from=n, country_code_to=bz, start=config.start, end=config.end, dayahead=dayahead)
            else:
                f_out = safe_query(client.query_crossborder_flows, context=f"{bz}->{n}", country_code_from=bz, country_code_to=n, start=config.start, end=config.end)
                f_in = safe_query(client.query_crossborder_flows, context=f"{n}->{bz}", country_code_from=n, country_code_to=bz, start=config.start, end=config.end)

            if f_out is not None: flow_df = pd.concat([flow_df, f_out.loc[~f_out.index.duplicated()].to_frame(name=f"{bz}_{n}")], axis=1)
            if f_in is not None: flow_df = pd.concat([flow_df, f_in.loc[~f_in.index.duplicated()].to_frame(name=f"{n}_{bz}")], axis=1)

        if flow_df is not None: 
            flow_df = flow_df.loc[~flow_df.index.duplicated(keep='first')]
            flow_df.to_csv(raw_dir / f"{bz}_raw_flows.csv")
            table_name = f"{bz}_raw_{flow_type}_flows{'_dayahead' if dayahead else ''}"

            df_to_timescale(flow_df, table_name, config.db_schema_name)


def process_flows(config: PipelineConfig, flow_type: str = "commercial", dayahead: bool = False) -> Dict[str, pd.DataFrame]:
    """
    Standardizes bilateral flow matrices, applies gap imputation, and evaluates zero-flow legitimacy.
    Operates in two phases to guarantee a globally synchronized data vintage.
    """
    folder = "physical_flow_data_bidding_zones" if flow_type == "physical" else f"comm_flow_{'dayahead' if dayahead else 'total'}_bidding_zones"
    raw_dir, out_dir, gaps_dir = config.get_output_path(folder) / "raw", config.get_output_path(folder), config.get_gaps_path(folder)
    
    flow_dict: Dict[str, pd.DataFrame] = {}
    vintages = []

    # ========================================================
    # PHASE 1: PROCESS AND COLLECT
    # ========================================================
    for bz in config.zones:
        table_name = f"raw_{flow_type}_flows" + ("_da" if dayahead else "")
        flow_path = raw_dir / f"{bz}_raw_flows.csv"

        df: Optional[pd.DataFrame] = io.load(flow_path, table_name, config, bz=bz)

        if df is not None:
            # 1. Primary: Check internal columns for data lineage
            if "download_timestamp" in df.columns:
                v = str(df["download_timestamp"].iloc[0]).split()[0]
            elif "source_download_date" in df.columns:
                v = str(df["source_download_date"].iloc[0]).split()[0]
            elif flow_path and flow_path.exists():
                v = datetime.fromtimestamp(flow_path.stat().st_mtime).strftime('%Y-%m-%d')
            else:
                v = pd.Timestamp.utcnow().strftime('%Y-%m-%d')

            vintages.append(v)
        else:
            logger.warning(f"No raw flow data found for {bz} in {raw_dir}. Skipping processing for this zone.")
            continue

        meta_cols = ["download_timestamp", "bidding_zone", "gap_filling_method"]
        data_cols = [c for c in df.columns if c not in meta_cols]

        # Isolate numerical features prior to resampling
        df[data_cols] = df[data_cols].apply(pd.to_numeric, errors='coerce')
        df_resampled = df.resample("1h").mean(numeric_only=True)

        df = fill_gaps_wrapper(df_resampled, gaps_dir, f"{bz}_flows", config=config, bz=bz, flow_type=flow_type, dayahead=dayahead)
        
        logger.info(f"[Process] {flow_type} flows for {bz} (Dayahead={dayahead})...")

        # Establish localized net border exports
        net_df = pd.DataFrame(index=df.index)
        for n in [z for z in config.neighbours_map[bz] if z in config.zones]:
            col_out = f"{bz}_{n}"
            col_in = f"{n}_{bz}"

            if col_out in df.columns or col_in in df.columns:
                out_series = df[col_out] if col_out in df.columns else 0.0
                in_series = df[col_in] if col_in in df.columns else 0.0
                net_df[f"{col_out}_net_export"] = out_series - in_series
        
        net_df["Net Export"] = net_df.sum(axis=1)

        # Handle systemic zero-drops and force net export recalculation
        final_df = correct_zero_values(pd.concat([df, net_df], axis=1), gaps_dir, bz, config, flow_type=flow_type)
        
        # Store in dictionary to await global synchronization
        flow_dict[bz] = final_df

    # ========================================================
    # PHASE 2: SYNCHRONIZE VINTAGE AND SAVE
    # ========================================================
    # Determines the latest temporal vintage across all flow matrices
    if vintages:
        current_batch_latest = max(vintages)
        existing_global = getattr(config, "analysis_source_date", current_batch_latest)
        config.analysis_source_date = max(existing_global, current_batch_latest)
        
        unique_dates = set(vintages)
        if len(unique_dates) > 1:
            logger.warning(f"Data vintage mismatch detected in {flow_type} flows: {unique_dates}. Synchronized to {config.analysis_source_date}.")
        else:
            logger.info(f"[Metadata] Flow data vintage synchronized to: {config.analysis_source_date}")

    # Commit synchronized dataframes to designated IO channels
    for bz, final_df in flow_dict.items():
        filename = f"{bz}_comm_flow_{'dayahead' if dayahead else 'total'}_bidding_zones.csv" if flow_type == "commercial" else f"{bz}_physical_flow_data_bidding_zones.csv"
        final_df.to_csv(out_dir / filename)
        df_to_timescale(final_df, timescale_table_name, config.db_schema_name)
        flow_dict[bz] = final_df

    return flow_dict

def balance_flows_symmetry(data_dict, config, flow_type="commercial", dayahead=False):
    """
    Enforces flow symmetry (A->B == B->A).
    Priority: If a zone has a processed '_zeros.csv' file, its data is treated as ground truth.
    """
    print(f"[Balance] Ensuring symmetry for {flow_type}...")
    
    # Setup paths
    folder = "physical_flow_data_bidding_zones" if flow_type == "physical" else f"comm_flow_{'dayahead' if dayahead else 'total'}_bidding_zones"
    suffix = f"_{folder}.csv"
    gaps_dir = config.get_gaps_path(folder)

    for bz in data_dict:
        # Check if this zone is a "Trusted Source" (has processed gaps file)
        bz_is_trusted = (gaps_dir / f"{bz}_zeros.csv").exists()

        for n in config.neighbours_map[bz]:
            if n not in data_dict: continue

            # Define columns
            bz_export, bz_import = f"{bz}_{n}", f"{n}_{bz}"
            n_export, n_import = f"{n}_{bz}", f"{bz}_{n}"

            if bz_export in data_dict[bz] and n_import in data_dict[n]:
                # Check mismatch
                if (data_dict[bz][bz_export] - data_dict[n][n_import]).abs().sum() > 1e-3:
                    
                    if bz_is_trusted:
                        # Case A: Trust BZ. Overwrite Neighbor.
                        data_dict[n][n_import] = data_dict[bz][bz_export].values
                        data_dict[n][n_export] = data_dict[bz][bz_import].values
                        
                        # Fix Neighbor's Net Export
                        if f"{n}_{bz}_net_export" in data_dict[n]:
                            data_dict[n][f"{n}_{bz}_net_export"] = data_dict[n][n_export] - data_dict[n][n_import]
                    else:
                        # Case B: Trust Neighbor. Overwrite BZ.
                        data_dict[bz][bz_export] = data_dict[n][n_import].values
                        data_dict[bz][bz_import] = data_dict[n][n_export].values

                        # Fix BZ's Net Export
                        if f"{bz}_{n}_net_export" in data_dict[bz]:
                            data_dict[bz][f"{bz}_{n}_net_export"] = data_dict[bz][bz_export] - data_dict[bz][bz_import]

    # Recalculate Total Net Exports and Save
    out_dir = config.get_output_path(folder)
    for bz, df in data_dict.items():
        net_cols = [c for c in df.columns if "net_export" in c and c != "Net Export"]
        if net_cols: df["Net Export"] = df[net_cols].sum(axis=1)
        df.to_csv(out_dir / f"{bz}{suffix}")

    return data_dict

# ==========================================
# GB HELPERS (BMRS API)
# ==========================================
def download_GB_per_type_data(start, end):
    """Fetches GB generation mix from BMRS."""
    all_days = pd.date_range(start=start.tz_convert("UTC"), end=end.tz_convert("UTC"), normalize=True, freq='D', tz="UTC")
    df_list = []
    for date in all_days:
        try:
            df_new = _download_GB_per_type_data(date)
            if df_new is not None: df_list.append(df_new)
        except Exception as e:
            print(f"Warning: Failed to fetch GB Gen for {date.date()}: {e}")
    return pd.concat(df_list).loc[start:end] if df_list else None

def download_GB_demand_data(start, end):
    """Fetches GB demand from BMRS."""
    all_days = pd.date_range(start=start.tz_convert("UTC"), end=end.tz_convert("UTC"), normalize=True, freq='D', tz="UTC")
    df_list = []
    for date in all_days:
        try:
            df_new = _download_GB_demand_data(date)
            if df_new is not None: df_list.append(df_new)
        except Exception as e:
            print(f"Warning: Failed to fetch GB Demand for {date.date()}: {e}")
    return pd.concat(df_list).loc[start:end] if df_list else None

def _download_GB_per_type_data(date):
    range_start = pd.Timestamp(f"{date.strftime('%Y-%m-%d')} 00:00", tz="UTC")
    range_end = pd.Timestamp(f"{date.strftime('%Y-%m-%d')} 23:30", tz="UTC")
    date_range = pd.date_range(start=range_start, end=range_end, freq="30min")

    # create new DataFrame with proper timestamps as index
    df = pd.DataFrame(index=date_range, columns=GB_GENERATION_TYPES)

    # URL to access per type generation
    url = f"https://data.elexon.co.uk/bmrs/api/v1/generation/actual/per-type?from={date.strftime('%Y-%m-%d')}T00:00&to={date.strftime('%Y-%m-%d')}T23:30&format=json"

    # Make a GET request to fetch the CSV data
    response = requests.get(url, timeout=TIMEOUT)

    # Check if the request was successful
    if response.status_code == 200:
        
        # Convert response to JSON format
        response = response.json()

        if len(response["data"]) > 0:
            # Read data into DataFrame
            data = pd.DataFrame(response["data"])

            # Set timestamp as index
            data.set_index("startTime", inplace=True)
            data.index = pd.to_datetime(data.index)
            
            # Iterate over each tech type in alphabetical order and extract associated data from the response
            for i, ttype in enumerate(sorted(GB_GENERATION_TYPES)):

                # Create a new column for each tech type extracting relevant data at each timestep
                data[ttype] = [sorted(data["data"].values[x], key=lambda d: d['psrType'])
                            [i]["quantity"] for x in range(len(data["data"].values))]
                
                # Copy data from response DataFrame to return DataFrame 
                df.loc[data.index, ttype] = data[ttype].values
    
    return df

def _download_GB_demand_data(date):
    range_start = pd.Timestamp(f"{date.strftime('%Y-%m-%d')} 00:00", tz="UTC")
    range_end = pd.Timestamp(f"{date.strftime('%Y-%m-%d')} 23:30", tz="UTC")
    date_range = pd.date_range(start=range_start, end=range_end, freq="30min")

    # create new DataFrame with proper timestamps as index
    df = pd.DataFrame(index=date_range, columns=["Actual Load"])

    # URL to access demand data
    url = f"https://data.elexon.co.uk/bmrs/api/v1/demand/actual/total?from={date.strftime('%Y-%m-%d')}T00:00&to={date.strftime('%Y-%m-%d')}T23:30&format=csv"
    
    # Make a GET request to fetch the CSV data
    response = requests.get(url, timeout=TIMEOUT)

    # Check if the request was successful
    if response.status_code == 200:
        # Use StringIO to read the CSV data into a DataFrame
        data = pd.read_csv(StringIO(response.text))

        # Sort data by timestamp
        data.sort_values(by=['StartTime'], inplace=True)

        # Set timestamp as index
        data.set_index(["StartTime"], inplace=True)
        data.index = pd.to_datetime(data.index)
        
        # add load data to DataFrame
        df.loc[data.index, "Actual Load"] = data["Quantity"].values

    return df

def fetch_simple_metrics(client, config):
    """Fetches Prices and Net Positions for Target Zones."""
    if not config.data_types["metrics"]: return
    for name, method, kwargs in [
        ("net_positions_dayahead", client.query_net_position, {"dayahead": True}),
        ("market_price_dayahead", client.query_day_ahead_prices, {})
    ]:
        out_dir = config.get_output_path(name)

        for bz in config.target_zones:
            logger.info(f"Fetching {name} for {bz}...")
            df = safe_query(method, context=f"{name} {bz}", country_code=bz, start=config.start, end=config.end, **kwargs)
            if df is not None:
                if isinstance(df, pd.Series):
                    df = df.to_frame(name="Value")

                # --- APPLY SIGN CORRECTION PATCH ---
                # Only apply to Net Positions, only for Italian zones, and only for the year 2025
                if name == "net_positions_dayahead" and bz.startswith("IT"):
                    # Check if the data falls in 2025 (using the index to be absolutely safe)
                    mask_2025 = df.index.year == 2025
                    if mask_2025.any():
                        print(
                            f"  -> [PATCH] Inverting signs for {bz} 2025 Net Positions due to ENTSO-E reporting error.")
                        df.loc[mask_2025] = df.loc[mask_2025] * -1
                # -----------------------------------

                df.index = pd.to_datetime(df.index, utc=True)
                df.resample("1h").mean().to_csv(out_dir / f"{bz}_{name}.csv")
                df_to_timescale(df, f"{bz}_{name}", config.db_schema_name, config)
                print(f"{bz}_{name}")