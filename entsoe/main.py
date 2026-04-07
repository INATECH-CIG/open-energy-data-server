import sys
from datetime import datetime
from entsoe import EntsoePandasClient
from config import PipelineConfig
from utils import start_logging

# --- MODULE IMPORTS ---
from download_data import (
    download_generation_demand,
    process_generation_demand,
    download_flows,
    process_flows,
    balance_flows_symmetry,
    fetch_simple_metrics
)
from data_analysis import (
    perform_decomposition_analysis, 
    perform_aggregated_flow_tracing,
    perform_direct_flow_tracing,
    perform_pooling_analysis,
    perform_post_processing_aggregation
)
from prefect import flow

@flow
def main():
    # ==========================================
    # CONTROL PANEL
    # ==========================================
    
    # 1. Execution Flags (True = Run this step)
    my_run_flags = {
        "download": True,
        "process": False,
        "analysis": False,
    }
    
    # 2. Define Period (UTC)
    period = ("2025-01-01 00:00", "2025-01-01 11:59")

    # 3. Optional: Download only Subsets of Data (Uncomment to use)
    # -------------------------------------------------------
    # selected_bzs = ["DE_LU", "FR", "GB"] 
    #
    # selected_data_types = {
    #     "generation": True,
    #     "flows_commercial_total": True,
    #     "flows_commercial_dayahead": True, # Download only
    #     "flows_physical": True,
    #     "metrics": False
    # }
    # -------------------------------------------------------

    # 4. Initialize Config
    config = PipelineConfig(
        date_range=period,
        run_flags=my_run_flags,
        debug_mode=False,
        db_schema_name= 'entsoe'
        # Uncomment below to apply subsets:
        # target_zones=selected_bzs,
        # data_types=selected_data_types
    )
    
    # 5. Setup Logging
    timestamp = datetime.now().strftime("%Y-%m-%d")
    timestamp_detailed = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    start_logging(config.base_dir / "logs" / f"log_{timestamp}" / f"log_{timestamp_detailed}.log")

    # ==========================================
    # PIPELINE EXECUTION
    # ==========================================

    # --- PHASE 1: DOWNLOAD ---
    if config.run_phases["download"]:
        print(f"=== STARTING DOWNLOAD ({config.start} to {config.end}) ===")
        client = EntsoePandasClient(api_key=config.api_key)
        
        download_generation_demand(client, config) #outputs/generation_demand_data_bidding_zones/2025/raw/
        download_flows(client, config, "commercial", dayahead=False) #outputs/comm_flow_total_bidding_zones/2025/raw
        download_flows(client, config, "commercial", dayahead=True) #outputs/comm_flow_dayahead_bidding_zones/2025/raw
        download_flows(client, config, "physical") # outputs/physical_flow_data_bidding_zones/raw
        fetch_simple_metrics(client, config)

    # --- PHASE 2: PROCESS ---
    gen_data, final_comm, final_phys = None, None, None
    if config.run_phases["process"]:
        print("\n=== STARTING PROCESSING ===")
        
        # A. Generation & Demand (Clean, Resample, Patch Gaps)
        gen_data = process_generation_demand(config) #outputs/generation_demand_data_bidding_zones/2025
        # B. Commercial Flows (Total) -> Balance & Keep
        raw_comm = process_flows(config, "commercial", dayahead=False) # outputs/comm_flow_total_bidding_zones/2025/
        final_comm = balance_flows_symmetry(raw_comm, config, "commercial", dayahead=False) # outputs/comm_flow_total_bidding_zones/2025/

        # B. Day Ahead Flows -> Balance & Save (discard memory)
        raw_da = process_flows(config, "commercial", dayahead=True) #outputs/comm_flow_dayahead_bidding_zones/2025
        balance_flows_symmetry(raw_da, config, "commercial", dayahead=True) #outputs/comm_flow_dayahead_bidding_zones/2025

        # C. Commercial Flows (Total) -> Balance & Keep
        raw_comm = process_flows(config, "commercial", dayahead=False)
        final_comm = balance_flows_symmetry(raw_comm, config, "commercial", dayahead=False)

        # D. Physical Flows -> Balance & Keep
        raw_phys = process_flows(config, "physical") # outputs/physical_flow_data_bidding_zones
        final_phys = balance_flows_symmetry(raw_phys, config, "physical") # outputs/physical_flow_data_bidding_zones

    # --- PHASE 3: ANALYSIS ---
    if config.run_phases["analysis"]:
        print("\n=== STARTING ANALYSIS ===")
        
        # 1. Neighbor Decomposition (Import Mix based on neighbors)
        perform_decomposition_analysis(config, gen_dfs=gen_data, comm_dfs=final_comm) # outputs/comm_flow_total_bidding_zones/2025/results/
        
        # 2. Flow Tracing (Matrix Inversion)
        perform_aggregated_flow_tracing(config, gen_dfs=gen_data, phys_flow_dfs=final_phys) #outputs/import_flow_tracing_bidding_zones/agg_coupling
        perform_direct_flow_tracing(config, gen_dfs=gen_data, phys_flow_dfs=final_phys) #outputs/import_flow_tracing_bidding_zones/direct_coupling
        
        # 3. Pooling (European Mix)
        perform_pooling_analysis(config, gen_dfs=gen_data, comm_dfs=final_comm, phys_flow_dfs=final_phys) #outputs/pooling
        
        # 4. Aggregation (Annual Totals)
        perform_post_processing_aggregation(config) #outputs/annual_totals_per_method

if __name__ == "__main__":
    main()