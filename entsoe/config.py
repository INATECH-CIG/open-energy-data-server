import yaml
import pandas as pd
from pathlib import Path
from typing import Dict, Optional, Tuple, List
from entsoe import mappings

class PipelineConfig:
    def __init__(
        self, 
        date_range: Tuple[str, str],
        key_file: str = "keys.yaml", 
        run_flags: Optional[Dict[str, bool]] = None,
        target_zones: Optional[List[str]] = None,
        data_types: Optional[Dict[str, bool]] = None
    ):
        # --- PATHS ---
        self.base_dir = Path(__file__).parent
        self.output_dir = self.base_dir / "outputs"
        self.input_dir = self.base_dir / "inputs"
        
        # --- TIME SETTINGS ---
        self.start = pd.Timestamp(date_range[0], tz="UTC")
        self.end = pd.Timestamp(date_range[1], tz="UTC")
        self.time_index = pd.date_range(start=self.start, end=self.end, freq="1h")
        self.year = self.start.year 
        
        # --- EXECUTION FLAGS ---
        self.run_phases = {
            "download": True, "process": True, 
            "analysis": True
        }
        if run_flags: self.run_phases.update(run_flags)

        # --- DATA FILTERS ---
        self.data_types = {
            "generation": True,
            "flows_commercial_total": True,
            "flows_commercial_dayahead": True,
            "flows_physical": True,
            "metrics": True
        }
        if data_types: self.data_types.update(data_types)

        # --- ZONE CONFIGURATION ---
        # Master list of all zones for analysis
        self.all_zones = list(mappings.NEIGHBOURS.copy().keys())
        self.neighbours_map = mappings.NEIGHBOURS.copy()
        self._filter_zones()

        # Subset list for downloading specific regions
        if target_zones:
            self.target_zones = [z for z in target_zones if z in self.all_zones]
            print(f"Configured for subset of zones: {self.target_zones}")
        else:
            self.target_zones = self.all_zones

        # --- API KEY ---
        with open(self.base_dir / key_file, "r") as f:
            self.api_key = yaml.safe_load(f)["entsoe-key"]
            
        # --- INPUT DATA ---
        self.gen_types_df = pd.read_csv(
            self.input_dir / "generation_data/gen_types_and_emission_factors.csv"
        )
        self.gen_types_list = self.gen_types_df["entsoe"].tolist()

    def _filter_zones(self):
        """Removes irrelevant or problematic zones from the master list."""
        to_remove = ["DE_AT_LU", "IE_SEM", "IE", "NIE", "MT", 
                     "IT", "IT_BRNN", "IT_ROSN", "IT_FOGN"]
        for z in to_remove:
            if z in self.all_zones: self.all_zones.remove(z)
            if z in self.neighbours_map: del self.neighbours_map[z]

    @property
    def zones(self):
        """Alias for all_zones, used during analysis to ensure full network coverage."""
        return self.all_zones

    def get_output_path(self, subfolder: str) -> Path:
        """Creates and returns year-specific output directories."""
        path = self.output_dir / subfolder / str(self.year)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def get_gaps_path(self, subfolder: str) -> Path:
        """Creates and returns directories for data gap logs."""
        path = self.output_dir / subfolder / str(self.year) / "gaps"
        path.mkdir(parents=True, exist_ok=True)
        return path