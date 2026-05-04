"""
Project: European Electricity Exchange Analysis
Author: Tiernan Buckley
Year: 2026
License: Creative Commons Attribution 4.0 International (CC BY 4.0)
Source: https://github.com/INATECH-CIG/exchange_analysis

Description:
Interactive Streamlit dashboard for visualizing the European Electricity Market 
Exchange Analysis. This module renders dynamic geographic flow maps, tracks 
bidding zone net positions, and plots high-resolution hourly internal generation 
and imported fuel mixes using Plotly and GeoPandas.
"""

import streamlit as st
import pandas as pd
import geopandas as gpd
import plotly.graph_objects as go
import json
import numpy as np
import os
import yaml
from pathlib import Path
from entsoe.geo.utils import load_zones
from oeds_dataio import OedsDataIO

# ==========================================
# 0. CONFIG & CONSTANTS
# ==========================================

# Defines logical database tables mapped to user-friendly UI names
FLOW_TYPES = {
    "Physical Cross-Border Flows": {
        "flow_table": "processed_physical_flows",
        "mix_table": None, 
        "type": "standard"
    },
    "Comm. Flows - Total (CFT)": {
        "flow_table": "processed_commercial_flows",
        "mix_table": "analysis_cft_netted_type",
        "type": "standard"
    },
    "Comm. Flows - Day-Ahead (CFD)": {
        "flow_table": "processed_commercial_flows_da",
        "mix_table": None,
        "type": "standard"
    },
    "Flow Tracing - Agg. Coupling": {
        "flow_table": "tracing_agg_coupling_bz",
        "export_table": "tracing_agg_coupling_bz_export",
        "mix_table": "tracing_agg_coupling_type",
        "type": "tracing"
    },
    "Flow Tracing - Direct Coupling": {
        "flow_table": "tracing_direct_coupling_bz",
        "export_table": "tracing_direct_coupling_bz_export",
        "mix_table": "tracing_direct_coupling_type",
        "type": "tracing"
    },
    "Net Pooled CFT": {
        "flow_table": "pool_commercial_net_pos_bz",
        "export_table": "pool_commercial_net_pos_bz_export",
        "mix_table": "pool_commercial_net_pos_type",
        "type": "tracing"
    }
}

GEN_COLORS = {
    "Solar": "#f1c40f", "Wind Onshore": "#3498db", "Wind Offshore": "#2980b9",
    "Biomass": "#27ae60", "Hydro Water Reservoir": "#1abc9c", 
    "Hydro Run-of-river and poundage": "#16a085", "Geothermal": "#d35400",
    "Marine": "#16a085", "Other renewable": "#2ecc71", "Nuclear": "#9b59b6",
    "Fossil Gas": "#e67e22", "Fossil Hard coal": "#34495e", "Fossil Oil": "#2c3e50",
    "Waste": "#7f8c8d", "Fossil Brown coal/Lignite": "#795548", 
    "Fossil Coal-derived gas": "#5d4037", "Fossil Oil shale": "#4e342e",
    "Fossil Peat": "#3e2723", "Storage Discharge": "#546e7a",
    "Storage Charge": "#78909c", "Storage": "#546e7a", "Other": "#bdc3c7"
}

# Map edge-case timezones; defaults to CET/CEST ('Europe/Berlin')
BZ_TIMEZONES = {
    'GB': 'Europe/London', 'IE': 'Europe/Dublin', 'IE_SEM': 'Europe/Dublin', 
    'PT': 'Europe/Lisbon', 'FI': 'Europe/Helsinki', 'EE': 'Europe/Tallinn', 
    'LV': 'Europe/Riga', 'LT': 'Europe/Vilnius', 'RO': 'Europe/Bucharest', 
    'BG': 'Europe/Sofia', 'GR': 'Europe/Athens', 'CY': 'Asia/Nicosia'
}

STICKY_HEADER_CSS = """
    <style>
        /* 1. Spacing configurations */
        .main > div {padding-left: 2rem; padding-right: 2rem; max-width: 100%;}
        .main .block-container { padding-top: 5rem; }
        [data-testid="stMetricDelta"] svg { display: none !important; }
        
        /* 2. Format native Streamlit header */
        [data-testid="stHeader"] { 
            background: rgba(255, 255, 255, 0.98) !important; 
            border-bottom: 1px solid #e6e6e6; 
            z-index: 99999;
        }
        
        /* 3. Inject absolute-positioned sticky title to stop flickering */
        [data-testid="stHeader"]::before {
            content: "European Electricity Market Exchange Analysis";
            position: absolute;
            left: 18rem; 
            top: 50%;
            transform: translateY(-50%); 
            font-size: 1.5rem;
            font-weight: bold;
            color: #1f2937;
            white-space: nowrap; 
            z-index: 1;
        }
        
        /* 4. Responsive logic for mobile / collapsed sidebar */
        @media (max-width: 991px) {
            [data-testid="stHeader"]::before { 
                left: 3.5rem; 
                font-size: 1.2rem;
            }
        }
    </style>
"""

class DashboardConfig:
    def __init__(self, selected_date: pd.Timestamp):
        self.start = pd.Timestamp(selected_date).tz_localize("UTC")
        self.end = self.start + pd.Timedelta(hours=23, minutes=59, seconds=59)

# ==========================================
# 1. TOPOLOGY & DATA IO INITIALIZATION
# ==========================================

def load_topology_config() -> dict:
    configured = os.getenv("TOPOLOGY_CONFIG_PATH")
    topology_path = Path(configured) if configured else Path(__file__).parent / "config" / "topology.yaml"
    
    if not topology_path.exists():
        st.error(f"Topology config not found at '{topology_path}'.")
        st.stop()
        
    data = yaml.safe_load(topology_path.read_text(encoding="utf-8")) or {}
    return data

try:
    TOPOLOGY_CONFIG = load_topology_config()
    NEIGHBOURS = TOPOLOGY_CONFIG["neighbours"]
except Exception as exc:
    st.error(str(exc))
    st.stop()

@st.cache_resource
def get_data_io():
    schema_name = os.getenv("DB_SCHEMA_NAME", "entsoe_tiernan")
    return OedsDataIO(schema_name=schema_name)

data_io = get_data_io()

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================

def get_clean_zones():
    to_remove = ["DE_AT_LU", "IE_SEM", "IE", "NIE", "MT", "IT", "IT_BRNN", "IT_ROSN", "IT_FOGN"]
    return sorted([z for z in NEIGHBOURS.keys() if z not in to_remove])

def get_bearing(lon1, lat1, lon2, lat2):
    d_lon = lon2 - lon1
    y = np.sin(np.radians(d_lon)) * np.cos(np.radians(lat2))
    x = (np.cos(np.radians(lat1)) * np.sin(np.radians(lat2)) - 
         np.sin(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.cos(np.radians(d_lon)))
    return (np.degrees(np.arctan2(y, x)) + 360) % 360

def get_curve(p1, p2, num_points=20):
    lons = np.linspace(p1[0], p2[0], num_points)
    lats = np.linspace(p1[1], p2[1], num_points)
    dist = np.sqrt((p2[0]-p1[0])**2 + (p2[1]-p1[1])**2)
    offset = dist * 0.15 
    shift = offset * np.sin(np.linspace(0, np.pi, num_points))
    return lons + shift*0.2, lats + shift

# ==========================================
# 3. DATA LOADING (Cached via DataIO)
# ==========================================

@st.cache_data
def load_geography(active_zones):
    custom_zones = ["GB", "ME", "BA", "MK"]
    entsoe_zones = [z for z in active_zones if z not in custom_zones]
    geo_df = load_zones(entsoe_zones, pd.Timestamp('2024-01-01'))
    input_dir = Path(os.getenv("STREAMLIT_INPUT_DIR", str(Path(__file__).parent.parent / "inputs")))
    
    for country in custom_zones:
        try:
            zone = gpd.read_file(input_dir / f"zones/{country}.geojson")
            geo_df.loc[country] = zone["geometry"][0]
        except: continue
        
    flat_centroids = geo_df.geometry.to_crs(epsg=3857).centroid.to_crs(epsg=4326)
    geo_df["lon"] = flat_centroids.x
    geo_df["lat"] = flat_centroids.y
    return geo_df.drop(["geometry"], axis=1), json.loads(geo_df.to_json())

@st.cache_data
def load_full_day_data(selected_date, target_bz, flow_settings, is_export=False):
    config = DashboardConfig(selected_date)
    table = flow_settings.get("export_table") if is_export else flow_settings.get("flow_table")
    if not table: return None
    return data_io.load(table, config.start, config.end, bz=target_bz)

@st.cache_data
def load_generation_data(selected_date, target_bz):
    config = DashboardConfig(selected_date)
    return data_io.load("processed_generation", config.start, config.end, bz=target_bz)

@st.cache_data
def load_import_mix(selected_date, target_bz, flow_settings):
    table_name = flow_settings.get("mix_table")
    if not table_name: return None
    config = DashboardConfig(selected_date)
    df = data_io.load(table_name, config.start, config.end, bz=target_bz)
    if df is not None and not df.empty:
        rename_map = {col: col.split("_")[-1] for col in df.columns if "_" in col}
        df = df.rename(columns=rename_map)
        return df.T.groupby(level=0).sum().T
    return df

def extract_arrow_flows(target_bz, hourly_all, active_zones, flow_settings, selected_date, target_time):
    active_flows = []
    if hourly_all is None or hourly_all.empty: 
        return active_flows
    
    if flow_settings.get("type") == "tracing":
        # 1. Imports: Read directly from the standard flow table
        for source_bz in active_zones:
            if source_bz == target_bz: 
                continue
            if source_bz in hourly_all.columns:
                val = hourly_all[source_bz].iloc[0]
                # Cast to float after verifying it is a BZ column to avoid string errors
                val_num = float(val) 
                if val_num > 0: 
                    active_flows.append({
                        "Source": source_bz, 
                        "Target": target_bz, 
                        "MW": val_num, 
                        "Type": "Import"
                    })
        
        # 2. Exports: Efficiently read from the newly generated export table
        export_df = load_full_day_data(selected_date, target_bz, flow_settings, is_export=True)
        if export_df is not None and target_time in export_df.index:
            hourly_exp = export_df[export_df.index == target_time]
            if not hourly_exp.empty:
                for target_other in active_zones:
                    if target_other == target_bz: 
                        continue
                    if target_other in hourly_exp.columns:
                        val = hourly_exp[target_other].iloc[0]
                        val_num = float(val)
                        if val_num > 0: 
                            active_flows.append({
                                "Source": target_bz, 
                                "Target": target_other, 
                                "MW": val_num, 
                                "Type": "Export"
                            })
    else:
        # Standard logic for Physical and Commercial methods
        for col in hourly_all.columns:
            val = hourly_all[col].iloc[0]
            
            # String-safe check: strings like '2026-05-04' won't equal 0 and won't crash
            if pd.isna(val) or val == 0: 
                continue
            
            if col in active_zones:
                val_num = float(val)
                direction = 'Export' if val_num > 0 else 'Import'
                s, t = (target_bz, col) if val_num > 0 else (col, target_bz)
                active_flows.append({
                    "Source": s, 
                    "Target": t, 
                    "MW": abs(val_num), 
                    "Type": direction
                })
                
            elif "_net_export" in col:
                val_num = float(val)
                pair = col.replace("_net_export", "")
                if pair.startswith(f"{target_bz}_"):
                    other = pair.replace(f"{target_bz}_", "")
                    direction = 'Import' if val_num < 0 else 'Export'
                    s, t = (other, target_bz) if val_num < 0 else (target_bz, other)
                elif pair.endswith(f"_{target_bz}"):
                    other = pair.replace(f"_{target_bz}", "")
                    direction = 'Export' if val_num < 0 else 'Import'
                    s, t = (target_bz, other) if val_num < 0 else (other, target_bz)
                
                active_flows.append({
                    "Source": s, 
                    "Target": t, 
                    "MW": abs(val_num), 
                    "Type": direction
                })
                
    return active_flows

# ==========================================
# 4. CHART & MAP GENERATORS
# ==========================================

def draw_flow_map(geo_df, geoj, flows, target_bz, net_position_gw):
    """Renders map with 'Badged' flow labels and explicit border/colors."""
    fig = go.Figure()
    relevant_lons = [geo_df.loc[target_bz, 'lon']]
    relevant_lats = [geo_df.loc[target_bz, 'lat']]
    
    # Define max bound for the color scale (e.g., 5 GW)
    max_gw = 5.0 
    
    # Base Choropleth Layer: Continuous scale based on net position
    fig.add_trace(go.Choropleth(
        geojson=geoj, locations=geo_df.index, 
        z=[(net_position_gw if i == target_bz else 0) for i in geo_df.index],
        zmin=-max_gw, zmax=max_gw,
        colorscale=[[0, "#a9cfe8"],    # Strong Import (Light Blue)
                    [0.5, '#e8ecef'],  # Neutral / Other Zones (Light Grey)
                    [1, "#9de4a3"]],   # Strong Export (Light Green)
        showscale=False, 
        marker_line_color=['#000000' if i == target_bz else '#adb5bd' 
                           for i in geo_df.index],
        marker_line_width=[2.0 if i == target_bz else 0.8 
                           for i in geo_df.index],
        text=[f"<b>{i}</b>" for i in geo_df.index], hoverinfo="text"
    ))
    
    COLOR_MAP = {'Export': {'l': 'rgba(40, 167, 69, 0.4)', 'a': '#28a745'},
                 'Import': {'l': 'rgba(0, 123, 255, 0.4)', 'a': '#007bff'}}
    
    for flow in flows:
        if flow['Source'] not in geo_df.index or flow['Target'] not in geo_df.index: 
            continue
        p1 = (geo_df.loc[flow['Source'], 'lon'], geo_df.loc[flow['Source'], 'lat'])
        p2 = (geo_df.loc[flow['Target'], 'lon'], geo_df.loc[flow['Target'], 'lat'])
        relevant_lons.extend([p1[0], p2[0]])
        relevant_lats.extend([p1[1], p2[1]])
        
        c = COLOR_MAP.get(flow['Type'])
        cLons, cLats = get_curve(p1, p2)
        
        # 1. Flow Vectors (Curved lines)
        fig.add_trace(go.Scattergeo(
            lon=cLons, lat=cLats, mode='lines', 
            line=dict(width=max(1.5, flow['MW']/500), color=c['l']), 
            hoverinfo='skip'
        ))
        
        mid = len(cLons)//2
        
        # 2. Value Badge: MW data in white background box
        fig.add_trace(go.Scattergeo(
            lon=[cLons[mid]], lat=[cLats[mid]], mode='markers+text',
            text=[f"<b>{flow['MW']/1000:.1f}</b>"], textposition="middle center",
            marker=dict(size=24, color='white', symbol='square', 
                        line=dict(color=c['a'], width=1)),
            textfont=dict(size=10, color='black', family="Arial"),
            hoverinfo='skip'
        ))

        # 3. Arrowhead: Direction indicator
        arr_idx = min(mid + 3, len(cLons)-1)
        fig.add_trace(go.Scattergeo(
            lon=[cLons[arr_idx]], lat=[cLats[arr_idx]], mode='markers',
            marker=dict(size=10, symbol='triangle-up', color=c['a'], 
                        angle=get_bearing(cLons[mid], cLats[mid], 
                                          cLons[arr_idx], cLats[arr_idx])),
            hoverinfo='skip'
        ))

    pad = 2.5
    fig.update_layout(
        geo=dict(projection_type="mercator", 
                 lonaxis_range=[min(relevant_lons)-pad, max(relevant_lons)+pad], 
                 lataxis_range=[min(relevant_lats)-pad, max(relevant_lats)+pad], 
                 visible=False), 
        margin={"r":0,"t":0,"l":0,"b":0}, height=650, 
        showlegend=False, clickmode='event+select'
    )
    return fig

def create_trend_chart(daily_trend, target_time_local, tz_str):
    """Generates the 24-hour net position trend bar chart in local time."""
    local_index = daily_trend.index.tz_convert(tz_str).tz_localize(None)
    
    fig = go.Figure(go.Bar(
        x=local_index, 
        y=round(daily_trend, 2), 
        marker_color=["#28a745" if v >= 0 else '#007bff' for v in daily_trend]
    ))
    fig.add_vline(x=target_time_local, line_width=2, line_dash="dash", line_color="#343a40")
    fig.update_layout(
        height=200, margin=dict(l=0, r=0, t=10, b=40), 
        xaxis=dict(title=f"Local Time ({tz_str})", tickformat="%H:%M"), 
        yaxis_title="GW", yaxis_zeroline=True, 
        yaxis_zerolinecolor='black', yaxis_zerolinewidth=1
    )
    return fig

def create_generation_chart(gen_df, target_time_local, tz_str):
    """Generates the localized internal generation mix and demand chart."""
    fig = go.Figure()
    local_index = gen_df.index.tz_convert(tz_str).tz_localize(None)
    
    pos_cols = [c for c in gen_df.columns if c in GEN_COLORS.keys() 
                and c not in ['Storage Charge', 'Total Load', 'Demand']]
    
    for c in pos_cols: 
        fig.add_trace(go.Scatter(
            x=local_index, y=round(gen_df[c]/1000, 2), name=c, mode='lines', 
            stackgroup='pos', line=dict(width=0, color=GEN_COLORS[c])
        ))
        
    if 'Storage Charge' in gen_df.columns:
        charge_vals = -np.abs(gen_df['Storage Charge']) / 1000
        fig.add_trace(go.Scatter(
            x=local_index, y=round(charge_vals, 2), name='Storage Charge', 
            mode='lines', stackgroup='neg', 
            line=dict(width=0, color=GEN_COLORS['Storage Charge']), 
            hovertemplate="%{customdata}", customdata=np.abs(round(charge_vals, 2))
        ))
        
    dem = next((c for c in gen_df.columns if c.lower() in 
                ['demand', 'total load']), None)
    if dem: 
        fig.add_trace(go.Scatter(
            x=local_index, y=round(gen_df[dem]/1000, 2), name='Demand', 
            line=dict(color='#2c3e50', width=3, dash='dot')
        ))
        
    fig.add_vline(x=target_time_local, line_width=2, line_dash="dash", line_color="#343a40")
    
    fig.update_layout(
        height=300, margin=dict(l=0, r=0, t=5, b=100), 
        xaxis=dict(title=f"Local Time ({tz_str})", tickformat="%H:%M"), 
        yaxis=dict(title="GW", zeroline=True, zerolinecolor='black', 
                   zerolinewidth=1), 
        legend=dict(orientation="h", yanchor="top", y=-0.8, 
                    xanchor="center", x=0.5), 
        hovermode="x unified"
    )
    return fig

def create_import_mix_chart(import_mix_df, target_time_local, tz_str):
    """Generates the traced imported fuel mix chart in local time."""
    fig = go.Figure()
    local_index = import_mix_df.index.tz_convert(tz_str).tz_localize(None)
    
    for c in [x for x in import_mix_df.columns if x in GEN_COLORS.keys()]:
        fig.add_trace(go.Scatter(
            x=local_index, y=round(import_mix_df[c]/1000, 2), name=c, 
            mode='lines', stackgroup='one', 
            line=dict(width=0, color=GEN_COLORS.get(c, "#95a5a6"))
        ))
        
    fig.add_vline(x=target_time_local, line_width=2, line_dash="dash", line_color="#343a40")
    
    fig.update_layout(
        height=300, margin=dict(l=0, r=0, t=10, b=100), 
        xaxis=dict(title=f"Local Time ({tz_str})", tickformat="%H:%M"), 
        yaxis=dict(title="GW", zeroline=True, zerolinecolor='black', 
                   zerolinewidth=1), 
        legend=dict(orientation="h", yanchor="top", y=-0.8, 
                    xanchor="center", x=0.5), 
        hovermode="x unified" 
    )
    return fig

# ==========================================
# 5. INTERFACE SETUP
# ==========================================

st.set_page_config(page_title="European Grid Analysis", layout="wide")
st.markdown(STICKY_HEADER_CSS, unsafe_allow_html=True)

# State initialization
active_zones = get_clean_zones()
if "target_bz" not in st.session_state: st.session_state.target_bz = "DE_LU"
if "hour_val" not in st.session_state: st.session_state.hour_val = 12
if "flow_method" not in st.session_state: st.session_state.flow_method = "Comm. Flows - Total (CFT)"

# Sidebar controls
st.sidebar.markdown("<h3 style='margin-top: -1.5rem; margin-bottom: 0;'>Select Data:</h3>", unsafe_allow_html=True)

selected_bz = st.sidebar.selectbox("Bidding Zone", active_zones, index=active_zones.index(st.session_state.target_bz))
if selected_bz != st.session_state.target_bz: 
    st.session_state.target_bz = selected_bz
    st.rerun()

date = st.sidebar.date_input("Day", pd.to_datetime("2026-03-07"))

# Compact divider
st.sidebar.markdown("<hr style='margin: 0.5rem 0; padding: 0;'>", unsafe_allow_html=True)

comm_methods = [
    "Comm. Flows - Total (CFT)", 
    "Comm. Flows - Day-Ahead (CFD)", 
    "Net Pooled CFT"
]
phys_methods = [
    "Physical Cross-Border Flows", 
    "Flow Tracing - Agg. Coupling", 
    "Flow Tracing - Direct Coupling"
]

# 1. Commercial Group
st.sidebar.markdown("**Commercial**")
c_idx = (comm_methods.index(st.session_state.flow_method) if st.session_state.flow_method in comm_methods else None)
c_sel = st.sidebar.radio("Commercial Methods", comm_methods, index=c_idx, label_visibility="collapsed")

# 2. Physical Group
st.sidebar.markdown("**Physical**")
p_idx = (phys_methods.index(st.session_state.flow_method) if st.session_state.flow_method in phys_methods else None)
p_sel = st.sidebar.radio("Physical Methods", phys_methods, index=p_idx, label_visibility="collapsed")

# 3. Synchronize state between the two lists
if c_sel and c_sel != st.session_state.flow_method and c_sel in comm_methods:
    st.session_state.flow_method = c_sel
    st.rerun()
elif p_sel and p_sel != st.session_state.flow_method and p_sel in phys_methods:
    st.session_state.flow_method = p_sel
    st.rerun()

selected_type = FLOW_TYPES[st.session_state.flow_method]

# ---------------------------------------------
# Local Time / UTC Select Slider
# ---------------------------------------------
tz_str = BZ_TIMEZONES.get(st.session_state.target_bz, 'Europe/Berlin')

utc_range = pd.date_range(start=date, periods=24, freq='h', tz='UTC')
local_range = utc_range.tz_convert(tz_str)

# Build unique slider labels showing both Local and UTC time mapping
time_labels = [f"{loc.strftime('%H:%M')} Local  |  {utc.strftime('%H:%M')} UTC" for loc, utc in zip(local_range, utc_range)]

selected_label = st.sidebar.select_slider(f"Selected Hour ({tz_str})", options=time_labels, value=time_labels[st.session_state.hour_val])

# Reverse lookup the correct UTC index for data fetching
hour = time_labels.index(selected_label)
st.session_state.hour_val = hour 

# ==========================================
# 6. RENDER DASHBOARD
# ==========================================

geo_data, geo_json = load_geography(active_zones)
full_day_df = load_full_day_data(date, st.session_state.target_bz, selected_type)

if full_day_df is not None and not full_day_df.empty:
    target_time = pd.to_datetime(f"{date} {st.session_state.hour_val:02d}:00:00").tz_localize('UTC')
    target_time_local = target_time.tz_convert(tz_str).tz_localize(None)
    
    # Calculate hourly Net Position dynamically based on DB structure
    if selected_type["type"] == "tracing":
        bz_cols = [c for c in full_day_df.columns if c in active_zones]
        h_imp = full_day_df[bz_cols].sum(axis=1) if bz_cols else pd.Series(0.0, index=full_day_df.index)
        
        h_exp = pd.Series(0.0, index=full_day_df.index)
        if selected_type.get("export_table"):
            export_df = load_full_day_data(date, st.session_state.target_bz, selected_type, is_export=True)
            if export_df is not None and not export_df.empty:
                exp_cols = [c for c in export_df.columns if c in active_zones]
                if exp_cols:
                    h_exp = h_exp.add(export_df[exp_cols].sum(axis=1), fill_value=0)
                
        daily_trend = h_exp.sub(h_imp, fill_value=0) / 1000
    else:
        if 'Net Export' in full_day_df.columns:
            daily_trend = full_day_df['Net Export'] / 1000
        else:
            bz_cols = [c for c in full_day_df.columns if c in active_zones]
            daily_trend = full_day_df[bz_cols].sum(axis=1) / 1000
    
    net_val = daily_trend.loc[target_time] if target_time in daily_trend.index else 0
    color = "#28a745" if net_val >= 0 else "#007bff"
    
    col_map, col_analysis = st.columns([65, 35], gap="large")

    with col_map:
        active_flows = extract_arrow_flows(st.session_state.target_bz, full_day_df[full_day_df.index == target_time], active_zones, selected_type, date, target_time)
        map_flows = [f for f in active_flows if f["MW"] > 10]
        map_event = st.plotly_chart(draw_flow_map(geo_data, geo_json, map_flows, st.session_state.target_bz, net_val), width="stretch", on_select="rerun", key="exchange_map")
        
        if map_event and 'selection' in map_event and map_event['selection']['points']:
            clicked = map_event['selection']['points'][0].get('location')
            if clicked in active_zones and clicked != st.session_state.target_bz: 
                st.session_state.target_bz = clicked
                st.rerun()
        
        with st.expander(f"📊 {st.session_state.flow_method} Flow Details"):
            if active_flows: 
                st.dataframe(round(pd.DataFrame(active_flows).sort_values(by="MW", ascending=False), 2), width="stretch", hide_index=True)
    
    with col_analysis:
        method_desc = {
            "Physical Cross-Border Flows": "Real-time metered physical power flows between adjacent zones.",
            "Comm. Flows - Total (CFT)": "Net scheduled commercial exchanges aggregated across all market timeframes.",
            "Comm. Flows - Day-Ahead (CFD)": "Net scheduled commercial exchanges explicitly from the Day-Ahead market.",
            "Flow Tracing - Agg. Coupling": "Traces flows assuming zones interact with the grid solely via their net position.",
            "Flow Tracing - Direct Coupling": "Traces flows using absolute internal generation and demand volumes as grid inputs.",
            "Net Pooled CFT": "Proportional allocation where theoretical net exporters supply all net importers."
        }
        
        badge_html = (
            f'<div style="background-color: #f8f9fa; border-left: 5px solid {color}; '
            f'padding: 10px; border-radius: 5px; margin-bottom: 15px;">'
            f'<small style="color: #6c757d; text-transform: uppercase; font-weight: bold;">'
            f'Current Methodology</small><br><span style="font-size: 1.1rem; font-weight: 500;">'
            f'{st.session_state.flow_method}</span><p style="margin: 0; font-size: 0.85rem; '
            f'color: #495057;">{method_desc.get(st.session_state.flow_method, "")}</p></div>'
        )
        st.markdown(badge_html, unsafe_allow_html=True)

        st.markdown(f"<style>[data-testid='stMetricDelta'] > div {{ color: {color} !important; }}</style>", unsafe_allow_html=True)
        st.metric(f"{st.session_state.target_bz} Net Position", f"{net_val:.2f} GW", f"Net {'Exporting' if net_val >= 0 else 'Importing'}")
        
        trend_fig = create_trend_chart(daily_trend, target_time_local, tz_str)
        st.plotly_chart(trend_fig, width="stretch", key="trend_chart")

        st.caption(f"🏠 {st.session_state.target_bz} Generation Mix & Demand")
        gen_df = load_generation_data(date, st.session_state.target_bz)
        
        if gen_df is not None and not gen_df.empty:
            gen_fig = create_generation_chart(gen_df, target_time_local, tz_str)
            st.plotly_chart(gen_fig, width="stretch", key="gen_chart")
        else:
            st.info(f"Generation mix data not available for {st.session_state.target_bz} on this date.")

        st.write("---")
        
        import_mix_df = load_import_mix(date, st.session_state.target_bz, selected_type)
        if import_mix_df is not None and not import_mix_df.empty:
            st.caption(f"🌍 {st.session_state.target_bz} Imported Energy Mix")
            imp_fig = create_import_mix_chart(import_mix_df, target_time_local, tz_str)
            st.plotly_chart(imp_fig, width="stretch", key="import_mix_chart")
        else: 
            st.info(f"Import decomposition unavailable for {st.session_state.flow_method}.")

else: 
    st.error(f"No exchange data available for {st.session_state.target_bz} on {date}. Map and metrics cannot be rendered.")
