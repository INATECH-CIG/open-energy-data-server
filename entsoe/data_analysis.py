import pandas as pd
import numpy as np
import os
from typing import Dict, Optional
from config import PipelineConfig

# ==========================================
# LOADER
# ==========================================
def _load_if_missing(config, gen_dfs, comm_dfs=None, phys_dfs=None):
    """
    Loads Generation, Commercial Flow, and Physical Flow data from disk if not passed in memory.
    Returns: Tuple(gen_dfs, comm_dfs, phys_dfs)
    """
    # 1. Load Generation Data
    if gen_dfs is None:
        print("Loading Gen data from disk...")
        gen_dir = config.get_output_path("generation_demand_data_bidding_zones")
        gen_dfs = {}
        for bz in config.zones:
            p = gen_dir / f"{bz}_generation_demand_data_bidding_zones.csv"
            if p.exists():
                df = pd.read_csv(p, index_col=0)
                df.index = pd.to_datetime(df.index, utc=True)
                gen_dfs[bz] = df.fillna(0)

    # 2. Load Commercial Flow Data
    if comm_dfs is None:
        print("Loading Commercial Flow data from disk...")
        comm_dir = config.get_output_path("comm_flow_total_bidding_zones")
        comm_dfs = {}
        for bz in config.zones:
            p = comm_dir / f"{bz}_commercial_flows_total_bidding_zones.csv"
            if p.exists():
                df = pd.read_csv(p, index_col=0)
                df.index = pd.to_datetime(df.index, utc=True)
                comm_dfs[bz] = df.fillna(0)

    # 3. Load Physical Flow Data
    if phys_dfs is None:
        print("Loading Physical Flow data from disk...")
        flow_dir = config.get_output_path("physical_flow_data_bidding_zones")
        phys_dfs = {}
        for bz in config.zones:
            p = flow_dir / f"{bz}_physical_flow_data_bidding_zones.csv"
            if p.exists():
                df = pd.read_csv(p, index_col=0)
                df.index = pd.to_datetime(df.index, utc=True)
                phys_dfs[bz] = df.fillna(0)
    
    return gen_dfs, comm_dfs, phys_dfs

# ==========================================
# PART 1: NEIGHBOR DECOMPOSITION
# ==========================================
def perform_decomposition_analysis(config: PipelineConfig, gen_dfs=None, comm_dfs=None):
    """
    Decomposes imports based on the immediate neighbor's generation mix.
    Calculates both 'Total' imports and 'Netted' imports (removing transit logic).
    """
    print("\n=== STARTING COMMERCIAL FLOW DECOMPOSITION ===")
    gen_dfs, comm_dfs, _ = _load_if_missing(config, gen_dfs, comm_dfs=comm_dfs)
    
    # Setup Output Directories
    agg_map = config.gen_types_df.groupby(['converted'])['entsoe'].apply(list).to_dict()
    base_out = config.output_dir / "comm_flow_total_bidding_zones" / str(config.year) / "results"
    
    dirs = {t: base_out / t for t in ["per_type_per_bidding_zone", "per_type", "per_agg_type", "per_bidding_zone"]}
    for k in list(dirs.keys()): dirs[f"netted_{k}"] = base_out / f"netted_{k}"
    for d in dirs.values(): d.mkdir(parents=True, exist_ok=True)

    # A. Prepare Netted Imports
    print("[Decomposition] Preparing Netted Import columns...")
    for bz, df in comm_dfs.items():
        net_cols = [col for col in df.columns if "net_export" in col and "Net Export" != col]
        for col in net_cols:
            target_col = col.replace("_net_export", "_netted_import")
            if target_col not in df.columns:
                df[target_col] = df[col].clip(upper=0).abs()
                
    print("[Decomposition] Saving raw flow totals (per_bidding_zone)...")
    
    for bz in comm_dfs:
        # 1. Setup Containers
        raw_total_imports = pd.DataFrame(index=config.time_index)
        raw_netted_imports = pd.DataFrame(index=config.time_index)
        
        # 2. Extract Columns
        # We look for columns like "FR_DE_LU" (Import to DE from FR) inside DE's dataframe
        for neighbor in config.zones:
            col_flow = f"{neighbor}_{bz}"
            col_netted = f"{bz}_{neighbor}_netted_import" # Note: Netted naming convention varies in your script, check this!
            
            # Save Raw Total Flow
            if col_flow in comm_dfs[bz].columns:
                raw_total_imports[neighbor] = comm_dfs[bz][col_flow]
                
            # Save Raw Netted Flow
            if col_netted in comm_dfs[bz].columns:
                raw_netted_imports[neighbor] = comm_dfs[bz][col_netted]

        # 3. Save Immediately
        if not raw_total_imports.empty:
            path_total = dirs["per_bidding_zone"] / f"{bz}_import_comm_flow_total_per_bidding_zone.csv"
            raw_total_imports.to_csv(path_total)
            
        if not raw_netted_imports.empty:
            path_netted = dirs["netted_per_bidding_zone"] / f"{bz}_import_comm_flow_total_netted_per_bidding_zone.csv"
            raw_netted_imports.to_csv(path_netted)
            
    print("[Decomposition] Raw totals saved. Proceeding to fuel type analysis...")

    # B. Calculate Generation Fractions
    print("[Decomposition] Calculating generation mix fractions...")
    gen_fractions = {}
    for bz, df in gen_dfs.items():
        cols = [c for c in df.columns if c in config.gen_types_list]
        total = df["Total Generation"].replace(0, 1) # Avoid div/0
        fracs = df[cols].div(total, axis=0)
        if "Storage Discharge" in df.columns:
            fracs["Hydro Pumped Storage"] = df["Storage Discharge"] / total
        gen_fractions[bz] = fracs

    # C. Perform Decomposition
    valid_zones = [z for z in config.zones if z in comm_dfs]
    print(f"[Decomposition] Processing {len(valid_zones)} zones...")
    
    for i, bz in enumerate(valid_zones):
        if i % 5 == 0: print(f"   -> Processing {bz}...")
        
        total_imp_list = []
        netted_imp_list = []
        
        # Iterate neighbors
        for neighbor in config.zones:
            col_flow = f"{neighbor}_{bz}"
            col_netted = f"{bz}_{neighbor}_netted_import"
            
            # Check for data existence
            if col_flow in comm_dfs[bz].columns:
                if neighbor in gen_fractions:
                    # 1. Total Imports * Neighbor Mix
                    t_df = gen_fractions[neighbor].mul(comm_dfs[bz][col_flow], axis=0)
                    t_df.columns = [f"{neighbor}_{c}" for c in t_df.columns]
                    total_imp_list.append(t_df)

                    # 2. Netted Imports * Neighbor Mix
                    if col_netted in comm_dfs[bz].columns:
                        n_df = gen_fractions[neighbor].mul(comm_dfs[bz][col_netted], axis=0)
                        n_df.columns = [f"{neighbor}_{c}" for c in n_df.columns]
                        netted_imp_list.append(n_df)
                else:
                    # Log missing neighbor mix (common issue)
                    # Only log if flow is non-zero to avoid spam
                    if comm_dfs[bz][col_flow].sum() > 0:
                        print(f"      [Warning] Flow exists from {neighbor}->{bz}, but no generation data for {neighbor}. Skipping.")

        if total_imp_list:
            total_full = pd.concat(total_imp_list, axis=1)
            netted_full = pd.concat(netted_imp_list, axis=1)
            
            # Save Raw Decomposed Data
            total_full.to_csv(dirs["per_type_per_bidding_zone"] / f"{bz}_import_comm_flow_total_per_type_per_bidding_zone.csv")
            netted_full.to_csv(dirs["netted_per_type_per_bidding_zone"] / f"{bz}_import_comm_flow_total_netted_per_type_per_bidding_zone.csv")
            
            # D. Aggregate by Fuel Type (e.g., all Wind entering DE)
            per_type = pd.DataFrame(index=config.time_index)
            per_type_net = pd.DataFrame(index=config.time_index)
            
            for tech in config.gen_types_list + ["Hydro Pumped Storage"]:
                cols = [c for c in total_full.columns if c.endswith(f"_{tech}") or c.endswith(f"_{tech} ")]
                cols_exact = [c for c in cols if c.split('_')[-1].strip() == tech]
                if cols_exact:
                    per_type[tech] = total_full[cols_exact].sum(axis=1)
                    per_type_net[tech] = netted_full[cols_exact].sum(axis=1)

            per_type.to_csv(dirs["per_type"] / f"{bz}_import_comm_flow_total_per_type.csv")
            per_type_net.to_csv(dirs["netted_per_type"] / f"{bz}_import_comm_flow_total_netted_per_type.csv")
            
            # E. Aggregate by Category (Renewable/Fossil)
            per_agg = pd.DataFrame(index=config.time_index)
            per_agg_net = pd.DataFrame(index=config.time_index)
            for cat, techs in agg_map.items():
                valid = [t for t in techs if t in per_type.columns]
                if valid:
                    per_agg[cat] = per_type[valid].sum(axis=1)
                    per_agg_net[cat] = per_type_net[valid].sum(axis=1)
            
            per_agg.to_csv(dirs["per_agg_type"] / f"{bz}_import_comm_flow_total_per_agg_type.csv")
            per_agg_net.to_csv(dirs["netted_per_agg_type"] / f"{bz}_import_comm_flow_total_netted_per_agg_type.csv")
            
    print("[Decomposition] Analysis Complete.")


# ==========================================
# PART 2 SHARED: TRACING SAVER
# ==========================================
def _decompose_and_save(config, traced_dfs, base_dir, label, gen_dfs):
    """
    Helper to apply generation mixes to Flow Tracing results (A^-1 * P).
    Aggregates by Zone, Type, and Category.
    """
    print(f"\n[{label.upper()}] Saving and Decomposing Traced Flows...")
    print(f"   -> Output Directory: {base_dir}")
    
    per_bz_dir = base_dir / "per_bidding_zone"
    per_type_dir = base_dir / "per_bidding_zone_per_type"
    per_type_total_dir = base_dir / "per_type"
    per_agg_total_dir = base_dir / "per_agg_type"
    for d in [per_bz_dir, per_type_dir, per_type_total_dir, per_agg_total_dir]: d.mkdir(parents=True, exist_ok=True)
    
    agg_map = config.gen_types_df.groupby(['converted'])['entsoe'].apply(list).to_dict()
    gen_fractions = {}
    
    # Calculate Mix Fractions
    print(f"   -> Calculating source generation fractions...")
    for bz, df in gen_dfs.items():
        cols = [c for c in df.columns if c in config.gen_types_list]
        total = df["Total Generation"].replace(0, 1)
        
        fracs = df[cols].div(total, axis=0) 
        if "Storage Discharge" in df.columns: 
            fracs["Hydro Pumped Storage"] = df["Storage Discharge"] / total
        
        gen_fractions[bz] = fracs

    total_zones = len(config.zones)
    count = 0
    
    for bz in config.zones:
        # Check if we have tracing results for this zone
        if bz not in traced_dfs or traced_dfs[bz].empty:
            continue
            
        count += 1
        if count % 5 == 0 or count == total_zones:
            print(f"      Saving results for {bz}...")

        # Save Volume coming from every other country (The "Flow Matrix" result)
        traced_dfs[bz].to_csv(per_bz_dir / f"{bz}_import_flow_tracing_{label}_per_bidding_zone.csv")
        
        # Multiply Volume by Origin's Mix
        type_dfs = []
        for n in config.zones:
            if n == bz: continue
            
            # Get flow volume from N -> BZ
            if n in traced_dfs[bz].columns:
                col_flow = traced_dfs[bz][n]
                
                # If we know N's mix, apply it
                if n in gen_fractions:
                    t_df = gen_fractions[n].mul(col_flow, axis=0)
                    t_df.columns = [f"{n}_{c}" for c in t_df.columns]
                    type_dfs.append(t_df)
        
        if type_dfs:
            full_type = pd.concat(type_dfs, axis=1)
            full_type.to_csv(per_type_dir / f"{bz}_import_flow_tracing_{label}_per_type_per_bidding_zone.csv")
            
            # Aggregate per Type
            per_type = pd.DataFrame(index=config.time_index)
            for tech in config.gen_types_list + ["Hydro Pumped Storage"]:
                cols = [c for c in full_type.columns if c.endswith(f"_{tech}") or c.endswith(f"_{tech} ")]
                cols_exact = [c for c in cols if c.split('_')[-1].strip() == tech]
                if cols_exact: per_type[tech] = full_type[cols_exact].sum(axis=1)
            per_type.to_csv(per_type_total_dir / f"{bz}_import_flow_tracing_{label}_per_type.csv")
            
            # Aggregate per Category
            per_agg = pd.DataFrame(index=config.time_index)
            for cat, techs in agg_map.items():
                valid = [t for t in techs if t in per_type.columns]
                if valid: per_agg[cat] = per_type[valid].sum(axis=1)
            per_agg.to_csv(per_agg_total_dir / f"{bz}_import_flow_tracing_{label}_per_agg_type.csv")
            
    print(f"[{label.upper()}] Save Complete.")

# ==========================================
# PART 2A: AGGREGATED FLOW TRACING
# ==========================================
def perform_aggregated_flow_tracing(config: PipelineConfig, gen_dfs=None, phys_flow_dfs=None):
    """
    Traces flows using Physical Flow Matrix Inversion (Q = A^-1 * Pin).
    Assumes perfect mixing within zones (Copper Plate).
    """
    print("Starting Aggregated Coupling Flow Tracing...")
    gen_dfs, _, phys_flow_dfs = _load_if_missing(config, gen_dfs, phys_dfs=phys_flow_dfs)
    
    # Resample to ensure alignment
    for bz in config.zones:
        if bz in gen_dfs: 
            if not isinstance(gen_dfs[bz].index, pd.DatetimeIndex): gen_dfs[bz].index = pd.to_datetime(gen_dfs[bz].index, utc=True)
            gen_dfs[bz] = gen_dfs[bz].resample("1h").mean().fillna(0)
        if bz in phys_flow_dfs: 
            if not isinstance(phys_flow_dfs[bz].index, pd.DatetimeIndex): phys_flow_dfs[bz].index = pd.to_datetime(phys_flow_dfs[bz].index, utc=True)
            phys_flow_dfs[bz] = phys_flow_dfs[bz].resample("1h").mean().fillna(0)

    agg_tracing = {bz: pd.DataFrame(0.0, index=config.time_index, columns=config.zones, dtype=float) for bz in config.zones}
    agg_dir = config.output_dir / "import_flow_tracing_bidding_zones/agg_coupling" / str(config.year)
    (agg_dir / "incalculable_timepoints").mkdir(parents=True, exist_ok=True)
    
    sing_times = []
    count = 0
    
    # Solve timestep by timestep
    for t in config.time_index:
        count += 1
        if count % 200 == 0: print(f"[Agg. Coupling] {t}")
        Pin, A, net_imps = [], [], []
        
        # Build System Matrices (A and Pin)
        for bz in config.zones:
            Pin_arr = [0] * len(config.zones)
            A_arr = [0] * len(config.zones)
            neighbours = [x for x in config.neighbours_map[bz] if x in config.zones and f"{bz}_{x}_net_export" in phys_flow_dfs[bz].columns]
            
            # Determine Outflows (A matrix diagonals/off-diagonals)
            exports = 0
            for n in neighbours:
                val = phys_flow_dfs[bz].at[t, f"{bz}_{n}_net_export"]
                if val < 0: A_arr[config.zones.index(n)] = val # Inflow
                else: exports += val # Outflow
            
            net_exp = phys_flow_dfs[bz].at[t, "Net Export"]
            if net_exp > 0:
                Pin_arr[config.zones.index(bz)] = net_exp
                A_arr[config.zones.index(bz)] = exports
                net_imps.append(0)
            else:
                A_arr[config.zones.index(bz)] = exports + abs(net_exp)
                net_imps.append(-net_exp) # Net Import
            
            Pin.append(Pin_arr)
            A.append(A_arr)
        
        # Solve Linear Equation
        try:
            A_inv = np.linalg.inv(A)
            q = np.dot(A_inv, Pin)
            for x in range(len(config.zones)):
                imps = net_imps[x] * q[x]
                for i in range(len(imps)):
                    if imps[i] > 0 and x != i:
                        agg_tracing[config.zones[x]].at[t, config.zones[i]] = imps[i]
        except:
            sing_times.append(t)
    
    pd.DataFrame(sing_times, columns=["Timepoints"]).to_csv(agg_dir / "incalculable_timepoints/incalculable_timepoints.csv")
    _decompose_and_save(config, agg_tracing, agg_dir, "agg_coupling", gen_dfs)

# ==========================================
# PART 2B: DIRECT FLOW TRACING
# ==========================================
def perform_direct_flow_tracing(config: PipelineConfig, gen_dfs=None, phys_flow_dfs=None):
    """
    Traces flows but links Generation directly to Demand in the matrix equation.
    Uses Total Generation + Imports to solve for Demand satisfaction.
    """
    print("Starting Direct Coupling Flow Tracing...")
    gen_dfs, _, phys_flow_dfs = _load_if_missing(config, gen_dfs, phys_dfs=phys_flow_dfs)
    
    # Resample alignment
    for bz in config.zones:
        if bz in gen_dfs: gen_dfs[bz] = gen_dfs[bz].resample("1h").mean().fillna(0)
        if bz in phys_flow_dfs: phys_flow_dfs[bz] = phys_flow_dfs[bz].resample("1h").mean().fillna(0)

    dir_tracing = {bz: pd.DataFrame(0.0, index=config.time_index, columns=config.zones, dtype=float) for bz in config.zones}
    dir_dir = config.output_dir / "import_flow_tracing_bidding_zones/direct_coupling" / str(config.year)
    (dir_dir / "incalculable_timepoints").mkdir(parents=True, exist_ok=True)
    
    sing_times = []
    count = 0
    
    # Solve timestep by timestep
    for t in config.time_index:
        count += 1
        if count % 200 == 0: print(f"[Direct Coupling] {t}")
        Gin, A, demands = [], [], []
        
        # Build Matrices using Generation (Gin) and Demand (calculated)
        for bz in config.zones:
            Gin_arr = [0] * len(config.zones)
            A_arr = [0] * len(config.zones)
            
            # 1. Calculate Candidates
            gen_val = gen_dfs[bz].at[t, "Total Generation"]
            load_val = gen_dfs[bz].at[t, "Total Load"]
            net_exp = phys_flow_dfs[bz].at[t, "Net Export"]
            
            alt_gen = load_val + net_exp
            alt_demand = gen_val - net_exp
            
            neighbours = [x for x in config.neighbours_map[bz] if x in config.zones and f"{bz}_{x}_net_export" in phys_flow_dfs[bz].columns]
            exports = 0
            for n in neighbours:
                val = phys_flow_dfs[bz].at[t, f"{bz}_{n}_net_export"]
                if val < 0: A_arr[config.zones.index(n)] = val # Inflow (Off-diagonal)
                else: exports += val # Outflow part of diagonal
            
            if load_val > 0:
                # PRIORITY 1: Trust Load + Flows (Recalculate Gen)
                Gin_arr[config.zones.index(bz)] = alt_gen if alt_gen > 0 else gen_val
                A_arr[config.zones.index(bz)] = load_val + exports
                demands.append(load_val)
            
            elif alt_demand > 0:
                # PRIORITY 2: Trust Gen + Flows (Recalculate Load)
                Gin_arr[config.zones.index(bz)] = gen_val 
                A_arr[config.zones.index(bz)] = alt_demand + exports
                demands.append(alt_demand)
            
            else:
                # Fallback to Flow-only (assume pure transit)
                Gin_arr[config.zones.index(bz)] = exports # Or 0, depending on preference
                A_arr[config.zones.index(bz)] = exports
                demands.append(0)
            
            Gin.append(Gin_arr)
            A.append(A_arr)
            
        try:
            A_inv = np.linalg.inv(A)
            q = np.dot(A_inv, Gin)
            for x in range(len(config.zones)):
                imps = demands[x] * q[x]
                for i in range(len(imps)):
                    if imps[i] > 0 and x != i:
                        dir_tracing[config.zones[x]].at[t, config.zones[i]] = imps[i]
        except:
            sing_times.append(t)
    
    pd.DataFrame(sing_times, columns=["Timepoints"]).to_csv(dir_dir / "incalculable_timepoints/incalculable_timepoints.csv")
    _decompose_and_save(config, dir_tracing, dir_dir, "direct_coupling", gen_dfs)

# ==========================================
# PART 3: POOLING (Hypothetical European Mix)
# ==========================================
def perform_pooling_analysis(config: PipelineConfig, gen_dfs=None, comm_dfs=None, phys_flow_dfs=None):
    """
    Calculates the 'European Mix' (Pooling) under three different assumptions.
    Assumes the entire continent is a single 'Copper Plate' (infinite transmission).
    """
    print("\n=== STARTING POOLING ANALYSIS ===")
    gen_dfs, comm_dfs, phys_flow_dfs = _load_if_missing(config, gen_dfs, comm_dfs, phys_flow_dfs)
    
    # ---------------------------------------------------------
    # 1. Calculate Generation Fractions
    # ---------------------------------------------------------
    print("[Pooling] 1/4: Calculating generation fractions for all zones...")
    gen_fractions = {}
    for i, (bz, df) in enumerate(gen_dfs.items()):
        # Optional: Print progress every 10 zones to avoid clutter
        if i % 10 == 0: print(f"   -> Processing fractions for {bz}...")
        
        cols = [c for c in df.columns if c in config.gen_types_list]
        total = df["Total Generation"].replace(0, 1)
        
        fracs = df[cols].div(total, axis=0) 
        if "Storage Discharge" in df.columns: 
            fracs["Hydro Pumped Storage"] = df["Storage Discharge"] / total
        
        gen_fractions[bz] = fracs.loc[:, ~fracs.columns.duplicated()].fillna(0.0)
        
    agg_map = config.gen_types_df.groupby(['converted'])['entsoe'].apply(list).to_dict()
    
    # Create Output Directory
    base_pool = config.output_dir / "pooling" / str(config.year)
    base_pool.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------
    # 2. Helper Function: Save Pooling Results
    # ---------------------------------------------------------
    def save_pool(pooled_dict, name, file_p):
        print(f"   -> Saving results for method: {name}")
        out = base_pool / name
        dirs = {k: out / k for k in ["per_bidding_zone", "per_type_per_bidding_zone", "per_type", "per_agg_type"]}
        for d in dirs.values(): d.mkdir(parents=True, exist_ok=True)
        
        count = 0
        total_zones = len(pooled_dict)
        
        for bz, df_imp in pooled_dict.items():
            count += 1
            # Print progress bar-style log
            if count % 5 == 0 or count == total_zones:
                print(f"      [{count}/{total_zones}] Saving {bz}...")

            # A. Save Raw Import Volumes
            df_imp.to_csv(dirs["per_bidding_zone"] / f"{bz}_pooled_{file_p}_per_bidding_zone.csv")
            
            # B. Apply Generation Fractions
            type_dfs = []
            for src in config.zones:
                if src in df_imp.columns and src in gen_fractions:
                    t = gen_fractions[src].mul(df_imp[src], axis=0)
                    t.columns = [f"{src}_{c}" for c in t.columns]
                    type_dfs.append(t)
            
            if type_dfs:
                full = pd.concat(type_dfs, axis=1)
                full.to_csv(dirs["per_type_per_bidding_zone"] / f"{bz}_pooled_{file_p}_per_type_per_bidding_zone.csv")
                
                # C. Aggregate by Fuel Type
                per_type = pd.DataFrame(index=config.time_index)
                for tech in config.gen_types_list + ["Hydro Pumped Storage"]:
                    cols = [c for c in full.columns if c.endswith(f"_{tech}") or c.endswith(f"_{tech} ")]
                    cols_exact = [c for c in cols if c.split('_')[-1].strip() == tech]
                    if cols_exact: per_type[tech] = full[cols_exact].sum(axis=1)
                per_type.to_csv(dirs["per_type"] / f"{bz}_pooled_{file_p}_per_type.csv")
                
                # D. Aggregate by Category
                per_agg = pd.DataFrame(index=config.time_index)
                for cat, techs in agg_map.items():
                    valid = [t for t in techs if t in per_type.columns]
                    if valid: per_agg[cat] = per_type[valid].sum(axis=1)
                per_agg.to_csv(dirs["per_agg_type"] / f"{bz}_pooled_{file_p}_per_agg_type.csv")

    # ---------------------------------------------------------
    # METHOD A: Commercial Link-Based Pooling
    # ---------------------------------------------------------
    print("\n[Pooling] 2/4: Calculating Commercial Link-Based Mix...")
    tot_exp, tot_imp = pd.DataFrame(index=config.time_index), pd.DataFrame(index=config.time_index)
    for bz in config.zones:
        if bz in comm_dfs:
            df = comm_dfs[bz]
            links = [c for c in df.columns if "net_export" in c and "Net Export" != c]
            tot_exp[bz] = df[links].clip(lower=0).sum(axis=1)
            tot_imp[bz] = df[links].clip(upper=0).abs().sum(axis=1)
            
    pool_mix = tot_exp.div(tot_exp.sum(axis=1).replace(0, 1), axis=0)
    save_pool({bz: pool_mix.mul(tot_imp[bz], axis=0) for bz in config.zones if bz in tot_imp}, "commercial_link_based", "imports")

    # ---------------------------------------------------------
    # METHOD B: Commercial Net Position Pooling
    # ---------------------------------------------------------
    print("\n[Pooling] 3/4: Calculating Commercial Net Position Mix...")
    net_exp, net_imp = pd.DataFrame(index=config.time_index), pd.DataFrame(index=config.time_index)
    for bz in config.zones:
        if bz in comm_dfs:
            v = comm_dfs[bz]["Net Export"]
            net_exp[bz], net_imp[bz] = v.clip(lower=0), v.clip(upper=0).abs()
            
    pool_mix_n = net_exp.div(net_exp.sum(axis=1).replace(0, 1), axis=0)
    save_pool({bz: pool_mix_n.mul(net_imp[bz], axis=0) for bz in config.zones if bz in net_imp}, "commercial_net_pos", "net_imports")

    # ---------------------------------------------------------
    # METHOD C: Physical Net Position Pooling
    # ---------------------------------------------------------
    print("\n[Pooling] 4/4: Calculating Physical Net Position Mix...")
    p_exp, p_imp = pd.DataFrame(index=config.time_index), pd.DataFrame(index=config.time_index)
    for bz in config.zones:
        if bz in phys_flow_dfs:
            v = phys_flow_dfs[bz]["Net Export"]
            p_exp[bz], p_imp[bz] = v.clip(lower=0), v.clip(upper=0).abs()
            
    pool_mix_p = p_exp.div(p_exp.sum(axis=1).replace(0, 1), axis=0)
    save_pool({bz: pool_mix_p.mul(p_imp[bz], axis=0) for bz in config.zones if bz in p_imp}, "physical_net_pos", "net_imports")
    
    print("\n[Pooling] Analysis Complete.")
    
# ==========================================
# PART 4: POST PROCESSING AGGREGATION
# ==========================================
def perform_post_processing_aggregation(config: PipelineConfig):
    """
    Aggregates all results (Decomposition, Tracing, Pooling) into Annual Totals (TWh).
    """
    print("\nStarting Post-Processing Aggregation...")
    base_out = config.output_dir
    year = str(config.year)
    paths = {
        "CFT": base_out / f"comm_flow_total_bidding_zones/{year}/results/per_bidding_zone",
        "Netted CFT": base_out / f"comm_flow_total_bidding_zones/{year}/results/netted_per_bidding_zone",
        "Pooled Net CFT": base_out / f"pooling/{year}/commercial_net_pos/per_bidding_zone",
        "Pooled Net Phys.": base_out / f"pooling/{year}/physical_net_pos/per_bidding_zone",
        "DC Flow Tracing": base_out / f"import_flow_tracing_bidding_zones/direct_coupling/{year}/per_bidding_zone",
        "AC Flow Tracing": base_out / f"import_flow_tracing_bidding_zones/agg_coupling/{year}/per_bidding_zone"
    }
    missing_path = base_out / f"import_flow_tracing_bidding_zones/agg_coupling/{year}/incalculable_timepoints/incalculable_timepoints.csv"
    
    totals_out = base_out / f"annual_totals_per_method/{year}"
    sub_outs = {k: totals_out / v for k,v in [("imp_bz", "import/per_bidding_zone"), ("exp_bz", "export/per_bidding_zone"), ("imp_type", "import/per_type"), ("exp_type", "export/per_type"), ("imp_agg", "import/per_agg_type")]}
    for p in sub_outs.values(): p.mkdir(parents=True, exist_ok=True)

    # Helper: Load and clean CSVs
    def load_clean(path, drop=None):
        if not path.exists(): return None
        df = pd.read_csv(path, index_col=0)
        df.index = pd.to_datetime(df.index, utc=True)
        df = df.loc[:, ~df.columns.duplicated()]
        df = df.apply(pd.to_numeric, errors='coerce').fillna(0.0)
        if drop and drop in df.columns: df = df.drop(columns=[drop])
        return df

    # Load missing timepoints (for gap patching)
    missing_times = []
    if missing_path.exists():
        missing_times = pd.to_datetime(pd.read_csv(missing_path)["Timepoints"], utc=True)

    # Reload Gen Data for Fractions
    gen_dir = config.get_output_path("generation_demand_data_bidding_zones")
    gen_fractions = {}
    for bz in config.zones:
        p = gen_dir / f"{bz}_generation_demand_data_bidding_zones.csv"
        if p.exists():
            df = load_clean(p)
            df = df.resample("1h").mean().fillna(0)
            cols = [c for c in df.columns if c in config.gen_types_list]
            total = df["Total Generation"].replace(0, 1)
            fracs = df[cols].div(total, axis=0)
            if "Storage Discharge" in df.columns: fracs["Hydro Pumped Storage"] = df["Storage Discharge"] / total
            gen_fractions[bz] = fracs.loc[:, ~fracs.columns.duplicated()].fillna(0.0)

    # Load All Results
    imports = {m: {} for m in paths}
    for bz in config.zones:
        imports["CFT"][bz] = load_clean(paths["CFT"] / f"{bz}_import_comm_flow_total_per_bidding_zone.csv", bz)
        imports["Netted CFT"][bz] = load_clean(paths["Netted CFT"] / f"{bz}_import_comm_flow_total_netted_per_bidding_zone.csv", bz)
        imports["Pooled Net CFT"][bz] = load_clean(paths["Pooled Net CFT"] / f"{bz}_pooled_net_imports_per_bidding_zone.csv", bz)
        imports["Pooled Net Phys."][bz] = load_clean(paths["Pooled Net Phys."] / f"{bz}_pooled_net_imports_per_bidding_zone.csv", bz)
        imports["DC Flow Tracing"][bz] = load_clean(paths["DC Flow Tracing"] / f"{bz}_import_flow_tracing_direct_coupling_per_bidding_zone.csv", bz)
        
        # Patch Missing AC data with Pooled data
        ac = load_clean(paths["AC Flow Tracing"] / f"{bz}_import_flow_tracing_agg_coupling_per_bidding_zone.csv", bz)
        if ac is not None and len(missing_times) > 0 and imports["Pooled Net Phys."][bz] is not None:
            ac.loc[ac.index.intersection(missing_times)] = imports["Pooled Net Phys."][bz].loc[ac.index.intersection(missing_times)]
        imports["AC Flow Tracing"][bz] = ac

    target_cols = list(set(config.gen_types_list + ["Hydro Pumped Storage"]))
    agg_map = config.gen_types_df.groupby(['converted'])['entsoe'].apply(list).to_dict()

    # Aggregate and Save
    for bz in config.zones:
        # Initialize float dtype DataFrames
        res_imp_bz = pd.DataFrame(dtype=float)
        res_exp_bz = pd.DataFrame(dtype=float)
        res_imp_type = pd.DataFrame(dtype=float)
        res_exp_type = pd.DataFrame(dtype=float)
        
        for m in paths:
            df_imp = imports[m].get(bz)
            if df_imp is None or df_imp.empty: continue
            
            # 1. Total Import per Method
            res_imp_bz.loc[m, df_imp.sum().index] = df_imp.sum()
            
            # 2. Import per Type (Apply Mix)
            imp_h = pd.DataFrame(0.0, index=config.time_index, columns=target_cols)
            for src in df_imp.columns:
                if src in gen_fractions:
                    mix = gen_fractions[src]
                    common = list(set(mix.columns) & set(imp_h.columns))
                    if common: imp_h[common] += mix[common].mul(df_imp[src], axis=0).fillna(0.0)
            res_imp_type.loc[m, imp_h.columns] = imp_h.sum()

            # 3. Export per Neighbor (Inverse of Import)
            h_exp = pd.Series(0.0, index=config.time_index)
            for n in config.zones:
                if n == bz: continue
                n_imp = imports[m].get(n)
                if n_imp is not None and bz in n_imp.columns:
                    flow = n_imp[bz].fillna(0.0)
                    res_exp_bz.loc[m, n] = flow.sum()
                    h_exp += flow
            
            # 4. Export per Type (Apply Own Mix)
            if bz in gen_fractions:
                decomp = gen_fractions[bz].mul(h_exp, axis=0)
                res_exp_type.loc[m, decomp.columns] = decomp.sum()

        # Convert MW -> TWh (Divide by 1e6) and Save
        res_imp_bz = res_imp_bz.apply(pd.to_numeric, errors='coerce').fillna(0.0) / 1e6
        res_exp_bz = res_exp_bz.apply(pd.to_numeric, errors='coerce').fillna(0.0) / 1e6
        res_imp_type = res_imp_type.apply(pd.to_numeric, errors='coerce').fillna(0.0) / 1e6
        res_exp_type = res_exp_type.apply(pd.to_numeric, errors='coerce').fillna(0.0) / 1e6
        
        res_imp_bz.T.to_csv(sub_outs["imp_bz"] / f"{bz}_annual_totals_import_per_bidding_zone_{year}.csv")
        res_exp_bz.T.to_csv(sub_outs["exp_bz"] / f"{bz}_annual_totals_export_per_bidding_zone_{year}.csv")
        res_imp_type.T.to_csv(sub_outs["imp_type"] / f"{bz}_annual_totals_import_per_type_{year}.csv")
        res_exp_type.T.to_csv(sub_outs["exp_type"] / f"{bz}_annual_totals_export_per_type_{year}.csv")
        
        # Save Aggregated Types
        res_agg = pd.DataFrame(dtype=float)
        for m in res_imp_type.index:
            for cat, techs in agg_map.items():
                valid = [t for t in techs if t in res_imp_type.columns]
                res_agg.loc[m, cat] = res_imp_type.loc[m, valid].sum()
        res_agg.T.to_csv(sub_outs["imp_agg"] / f"{bz}_annual_totals_import_per_agg_type_{year}.csv")
    
    print("Post-Processing Complete.")