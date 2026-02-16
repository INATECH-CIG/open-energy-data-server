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
)
from data_analysis import (
    perform_decomposition_analysis, 
    perform_aggregated_flow_tracing,
    perform_direct_flow_tracing,
    perform_pooling_analysis,
    perform_post_processing_aggregation
)

def main():
    # ==========================================
    # CONTROL PANEL
    # ==========================================
    
    # 1. Execution Flags (True = Run this step)
    my_run_flags = {
        "download": False,
        "process": False,
        "analysis": True,
    }
    
    # 2. Define Period (UTC)
    period = ("2025-01-01 00:00", "2025-01-01 23:59")

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
        
        download_generation_demand(client, config)
        download_flows(client, config, "commercial", dayahead=False)
        download_flows(client, config, "commercial", dayahead=True)
        download_flows(client, config, "physical")

    # --- PHASE 2: PROCESS ---
    gen_data, final_comm, final_phys = None, None, None
    if config.run_phases["process"]:
        print("\n=== STARTING PROCESSING ===")
        
        # A. Generation & Demand (Clean, Resample, Patch Gaps)
        gen_data = process_generation_demand(config)
        
        # B. Commercial Flows (Total) -> Balance & Keep
        raw_comm = process_flows(config, "commercial", dayahead=False)
        final_comm = balance_flows_symmetry(raw_comm, config, "commercial", dayahead=False)
        
        # C. Day Ahead Flows -> Balance & Save (discard memory)
        raw_da = process_flows(config, "commercial", dayahead=True)
        balance_flows_symmetry(raw_da, config, "commercial", dayahead=True)
        
        # D. Physical Flows -> Balance & Keep
        raw_phys = process_flows(config, "physical")
        final_phys = balance_flows_symmetry(raw_phys, config, "physical")

    # --- PHASE 3: ANALYSIS ---
    if config.run_phases["analysis"]:
        print("\n=== STARTING ANALYSIS ===")
        
        # 1. Neighbor Decomposition (Import Mix based on neighbors)
        perform_decomposition_analysis(config, gen_dfs=gen_data, comm_dfs=final_comm)
        
        # 2. Flow Tracing (Matrix Inversion)
        perform_aggregated_flow_tracing(config, gen_dfs=gen_data, phys_flow_dfs=final_phys)
        perform_direct_flow_tracing(config, gen_dfs=gen_data, phys_flow_dfs=final_phys)
        
        # 3. Pooling (European Mix)
        perform_pooling_analysis(config, gen_dfs=gen_data, comm_dfs=final_comm, phys_flow_dfs=final_phys)
        
        # 4. Aggregation (Annual Totals)
        perform_post_processing_aggregation(config)

if __name__ == "__main__":
    main()