import pandas as pd
import requests
from io import StringIO
from typing import Dict
from entsoe import EntsoePandasClient
from config import PipelineConfig
from utils import safe_query, fill_gaps_wrapper, correct_zero_values

# --- GB SPECIAL CONSTANTS ---
TIMEOUT = 60
GB_GENERATION_TYPES = [
    "Biomass", "Fossil Hard coal", "Fossil Gas", "Fossil Oil", 
    "Hydro Pumped Storage", "Hydro Run-of-river and poundage", 
    "Nuclear", "Other", "Solar", "Wind Offshore", "Wind Onshore"
]

# ==========================================
# GENERATION & DEMAND
# ==========================================

def download_generation_demand(client: EntsoePandasClient, config: PipelineConfig):
    """Downloads raw Gen/Demand data. Iterates strictly over TARGET ZONES."""
    if not config.data_types["generation"]: return

    raw_dir = config.get_output_path("generation_demand_data_bidding_zones") / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    for bz in config.target_zones:
        print(f"[Download] Gen/Demand for {bz}...")
        gen_df, load_df = None, None

        if bz == "GB":
            try:
                print("   -> Using BMRS API for GB...")
                gen_df = download_GB_per_type_data(config.start, config.end)
                load_df = download_GB_demand_data(config.start, config.end)
            except Exception as e:
                print(f"[Error] Failed to download GB data: {e}")
        else:
            gen_df = safe_query(client.query_generation, context=f"Generation {bz}", country_code=bz, start=config.start, end=config.end, nett=True)
            load_df = safe_query(client.query_load, context=f"Load {bz}", country_code=bz, start=config.start, end=config.end)

        if gen_df is not None: gen_df.to_csv(raw_dir / f"{bz}_raw_generation.csv")
        if load_df is not None: load_df.to_csv(raw_dir / f"{bz}_raw_load.csv")

def process_generation_demand(config: PipelineConfig) -> Dict[str, pd.DataFrame]:
    """Cleans/Resamples data. Iterates over ALL ZONES to build full dataset."""
    raw_dir = config.get_output_path("generation_demand_data_bidding_zones") / "raw"
    out_dir = config.get_output_path("generation_demand_data_bidding_zones")
    gaps_dir = config.get_gaps_path("generation_demand_data_bidding_zones")
    gen_storage_dict = {}

    for bz in config.zones:
        gen_path = raw_dir / f"{bz}_raw_generation.csv"
        load_path = raw_dir / f"{bz}_raw_load.csv"
        if not gen_path.exists() and not load_path.exists(): continue

        print(f"[Process] Gen/Demand for {bz}...")
        gen_df = pd.read_csv(gen_path, index_col=0) if gen_path.exists() else None
        load_df = pd.read_csv(load_path, index_col=0) if load_path.exists() else None

        if gen_df is not None:
            gen_df.index = pd.to_datetime(gen_df.index, utc=True)
            gen_df = gen_df.loc[:, ~gen_df.columns.duplicated()].apply(pd.to_numeric, errors='coerce')
            gen_df = gen_df.resample("1h").mean()
            
            # --- GAP FILLING (Generation) ---
            # Now passes directory and prefix directly
            gen_df = fill_gaps_wrapper(gen_df, gaps_dir, f"{bz}_gen")

            # Separate Storage
            if "Hydro Pumped Storage" in gen_df.columns:
                storage = gen_df["Hydro Pumped Storage"]
                gen_df = gen_df.drop(columns=["Hydro Pumped Storage"])
                gen_df["Generation"] = gen_df.sum(axis=1)
                gen_df["Storage Discharge"] = storage.clip(lower=0.0)
                gen_df["Storage Charge"] = storage.clip(upper=0.0).abs()
            else:
                gen_df["Generation"] = gen_df.sum(axis=1)
                gen_df["Storage Discharge"] = 0.0; gen_df["Storage Charge"] = 0.0

        if load_df is not None:
            load_df.index = pd.to_datetime(load_df.index, utc=True)
            load_df = load_df.apply(pd.to_numeric, errors='coerce').resample("1h").mean()
            
            # --- GAP FILLING (Load) ---
            load_df = fill_gaps_wrapper(load_df, gaps_dir, f"{bz}_load")

        if gen_df is not None:
            if load_df is not None:
                col_map = {"Actual Load": "Demand", "Load": "Demand"}
                for k, v in col_map.items():
                    if k in load_df.columns: gen_df["Demand"] = load_df[k]
                
                gen_df["Total Generation"] = gen_df["Generation"] + gen_df["Storage Discharge"]
                gen_df["Total Load"] = gen_df.get("Demand", 0) + gen_df["Storage Charge"]
                gen_df["Net Export"] = gen_df["Total Generation"] - gen_df["Total Load"]
            
            # Zero Correction (1-week patch)
            gen_df = correct_zero_values(gen_df, gaps_dir, bz, config)
            
            gen_df.to_csv(out_dir / f"{bz}_generation_demand_data_bidding_zones.csv")
            gen_storage_dict[bz] = gen_df

    return gen_storage_dict

# ==========================================
# FLOWS
# ==========================================

def download_flows(client: EntsoePandasClient, config: PipelineConfig, flow_type: str = "commercial", dayahead: bool = False):
    """
    Downloads raw commercial or physical flows. 
    Iterates strictly over TARGET ZONES as defined in config.
    """
    # Check if this data type is enabled in config
    if flow_type == "commercial" and not config.data_types.get(f"flows_commercial{'_dayahead' if dayahead else '_total'}"): return
    if flow_type == "physical" and not config.data_types.get("flows_physical"): return

    # Determine output folder
    if flow_type == "physical":
        folder = "physical_flow_data_bidding_zones"
    else:
        folder = f"comm_flow_{'dayahead' if dayahead else 'total'}_bidding_zones"
        
    raw_dir = config.get_output_path(folder) / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    for bz in config.target_zones:
        print(f"[Download] {flow_type} flows for {bz} (Dayahead={dayahead})...")
        flow_df = None
        
        # Iterate over neighbors
        for n in [z for z in config.neighbours_map[bz] if z in config.zones]:
            out_label = f"{flow_type} {bz}->{n}"
            in_label = f"{flow_type} {n}->{bz}"
            
            # Fetch Outgoing and Incoming flows
            if flow_type == "commercial":
                f_out = safe_query(client.query_scheduled_exchanges, context=out_label, country_code_from=bz, country_code_to=n, start=config.start, end=config.end, dayahead=dayahead)
                f_in = safe_query(client.query_scheduled_exchanges, context=in_label, country_code_from=n, country_code_to=bz, start=config.start, end=config.end, dayahead=dayahead)
            else:
                f_out = safe_query(client.query_crossborder_flows, context=out_label, country_code_from=bz, country_code_to=n, start=config.start, end=config.end)
                f_in = safe_query(client.query_crossborder_flows, context=in_label, country_code_from=n, country_code_to=bz, start=config.start, end=config.end)

            # Concatenate flows, ensuring no duplicate indices interfere
            if f_out is not None: 
                f_out = f_out.loc[~f_out.index.duplicated(keep='first')]
                flow_df = pd.concat([flow_df, f_out.to_frame(name=f"{bz}_{n}")], axis=1)
            
            if f_in is not None: 
                f_in = f_in.loc[~f_in.index.duplicated(keep='first')]
                flow_df = pd.concat([flow_df, f_in.to_frame(name=f"{n}_{bz}")], axis=1)

        # Save raw file if data was found
        if flow_df is not None: 
            flow_df = flow_df.loc[~flow_df.index.duplicated(keep='first')]
            flow_df.to_csv(raw_dir / f"{bz}_raw_flows.csv")

def process_flows(config: PipelineConfig, flow_type: str = "commercial", dayahead: bool = False) -> Dict[str, pd.DataFrame]:
    """Processes flows for ALL ZONES."""
    
    if flow_type == "physical":
        folder = "physical_flow_data_bidding_zones"
    else:
        folder = f"comm_flow_{'dayahead' if dayahead else 'total'}_bidding_zones"
        
    raw_dir, out_dir = config.get_output_path(folder) / "raw", config.get_output_path(folder)
    gaps_dir = config.get_gaps_path(folder)
    flow_dict = {}

    for bz in config.zones:
        raw_path = raw_dir / f"{bz}_raw_flows.csv"
        if not raw_path.exists(): continue

        print(f"[Process] {flow_type} flows for {bz}...")
        df = pd.read_csv(raw_path, index_col=0)
        df.index = pd.to_datetime(df.index, utc=True)
        df = df.resample("1h").mean()
        
        # --- GAP FILLING (Flows) ---
        df = fill_gaps_wrapper(df, gaps_dir, f"{bz}_flows", config=config, bz=bz, 
                               flow_type=flow_type, dayahead=dayahead)
        
        # Calculate Net Exports per neighbor
        net_df = pd.DataFrame(index=df.index)
        for n in [z for z in config.neighbours_map[bz] if z in config.zones]:
            if f"{bz}_{n}" in df.columns and f"{n}_{bz}" in df.columns:
                net_df[f"{bz}_{n}_net_export"] = df[f"{bz}_{n}"] - df[f"{n}_{bz}"]
        
        net_df["Net Export"] = net_df.sum(axis=1)
        final_df = pd.concat([df, net_df], axis=1)
        
        # Zero Correction (1-week patch)
        final_df = correct_zero_values(final_df, gaps_dir, bz, config)
        
        filename = f"{bz}_commercial_flows_{'dayahead' if dayahead else 'total'}_bidding_zones.csv" if flow_type == "commercial" else f"{bz}_physical_flow_data_bidding_zones.csv"
        final_df.to_csv(out_dir / filename)
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
    for name, method, kwargs in [("net_positions_dayahead", client.query_net_position, {"dayahead":True}), ("market_price_dayahead", client.query_day_ahead_prices, {})]:
        out_dir = config.get_output_path(name)
        for bz in config.target_zones:
            print(f"Fetching {name} for {bz}...")
            df = safe_query(method, context=f"{name} {bz}", country_code=bz, start=config.start, end=config.end, **kwargs)
            if df is not None:
                if isinstance(df, pd.Series): df = df.to_frame(name="Value")
                df.index = pd.to_datetime(df.index, utc=True)
                df.resample("1h").mean().to_csv(out_dir / f"{bz}_{name}.csv")