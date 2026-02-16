import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import sys

from entsoe import mappings

from matplotlib import colors
#%%
print("This script saves plots but intentionally doesn't display them, please go to 'annual_totals_per_method' to see saved plots.")
#%%
#Set filepaths

sys.path.append(os.path.dirname(__file__))

outputs_dir = os.path.join(os.path.dirname(__file__), "outputs/")

if not os.path.exists(outputs_dir):
    os.makedirs(outputs_dir)

range_start = pd.Timestamp("2025-01-01 00:00", tz="UTC")
range_end = pd.Timestamp("2025-12-31 23:59", tz="UTC")
time_range = pd.date_range(start=range_start, end=range_end, freq="1h")
#%%
gen_types_df = pd.read_csv(
    "inputs/generation_data/gen_types_and_emission_factors.csv"
)

gen_types_list = gen_types_df["entsoe"].tolist()
#%%
bidding_zones = list(mappings.NEIGHBOURS.copy().keys())
bidding_zones_dict = mappings.NEIGHBOURS.copy()
bidding_zones.remove("DE_AT_LU")
bidding_zones.remove("IE_SEM")
bidding_zones.remove("MT")
bidding_zones.remove("IT")
bidding_zones.remove("IT_BRNN")
bidding_zones.remove("IT_ROSN")
bidding_zones.remove("IT_FOGN")
bidding_zones.remove("IE")
bidding_zones.remove("NIE")

del bidding_zones_dict["DE_AT_LU"]
del bidding_zones_dict["IE_SEM"]
del bidding_zones_dict["MT"]
del bidding_zones_dict["IT"]
del bidding_zones_dict["IT_BRNN"]
del bidding_zones_dict["IT_ROSN"]
del bidding_zones_dict["IT_FOGN"]
del bidding_zones_dict["IE"]
del bidding_zones_dict["NIE"]
#%%
gen_dfs = {}
gen_demand_dir = os.path.join(outputs_dir, "generation_demand_data_bidding_zones/{}".format(range_start.strftime('%Y')))
        
for country in bidding_zones:
    
    if os.path.exists(os.path.join(gen_demand_dir, "{}_generation_demand_data_bidding_zones.csv".format(country))):
        gen_dfs[country] = pd.read_csv(os.path.join(gen_demand_dir, "{}_generation_demand_data_bidding_zones.csv".format(country)), index_col=0)
        gen_dfs[country].index = pd.to_datetime(gen_dfs[country].index, utc=True)

#%%
gen_dfs_fractions = {}
for bz, df in gen_dfs.items():
    cols = [c for c in df.columns if c in gen_types_list]
    total = df["Total Generation"].replace(0, 1) # Avoid div/0
    fracs = df[cols].div(total, axis=0)
    if "Storage Discharge" in df.columns:
        fracs["Hydro Pumped Storage"] = df["Storage Discharge"] / total
    gen_dfs_fractions[bz] = fracs
#%%
annual_totals_import_per_bidding_zone_dir = os.path.join(outputs_dir, "annual_totals_per_method/{}/import/per_bidding_zone".format(range_start.strftime('%Y')))
annual_totals_export_per_bidding_zone_dir = os.path.join(outputs_dir, "annual_totals_per_method/{}/export/per_bidding_zone".format(range_start.strftime('%Y')))
annual_totals_import_per_type_dir = os.path.join(outputs_dir, "annual_totals_per_method/{}/import/per_type".format(range_start.strftime('%Y')))
annual_totals_import_per_agg_type_dir = os.path.join(outputs_dir, "annual_totals_per_method/{}/import/per_agg_type".format(range_start.strftime('%Y')))
annual_totals_export_per_type_dir = os.path.join(outputs_dir, "annual_totals_per_method/{}/export/per_type".format(range_start.strftime('%Y')))

export_per_type_methods = {}
export_per_bidding_zone_methods = {}
import_per_type_methods = {}
import_per_agg_type_methods = {}
import_per_bidding_zone_methods = {}

for country in bidding_zones:
    export_per_type_methods[country] = pd.read_csv(os.path.join(annual_totals_export_per_type_dir, "{}_annual_totals_export_per_type_{}.csv".format(country, range_start.strftime('%Y'))), index_col=0)
    export_per_bidding_zone_methods[country] = pd.read_csv(os.path.join(annual_totals_export_per_bidding_zone_dir, "{}_annual_totals_export_per_bidding_zone_{}.csv".format(country, range_start.strftime('%Y'))), index_col=0)
    import_per_type_methods[country] = pd.read_csv(os.path.join(annual_totals_import_per_type_dir, "{}_annual_totals_import_per_type_{}.csv".format(country, range_start.strftime('%Y'))), index_col=0)
    import_per_agg_type_methods[country] = pd.read_csv(os.path.join(annual_totals_import_per_agg_type_dir, "{}_annual_totals_import_per_agg_type_{}.csv".format(country, range_start.strftime('%Y'))), index_col=0)
    import_per_bidding_zone_methods[country] = pd.read_csv(os.path.join(annual_totals_import_per_bidding_zone_dir, "{}_annual_totals_import_per_bidding_zone_{}.csv".format(country, range_start.strftime('%Y'))), index_col=0)
    
    export_per_type_methods[country] = export_per_type_methods[country].T
    export_per_bidding_zone_methods[country] = export_per_bidding_zone_methods[country].T
    import_per_type_methods[country] = import_per_type_methods[country].T
    import_per_agg_type_methods[country] = import_per_agg_type_methods[country].T
    import_per_bidding_zone_methods[country] = import_per_bidding_zone_methods[country].T
#%%
import_method_types = ["CFT", "Netted CFT", "Net Imports CFT \n(Pooled)",
                       "Net Imports Phys. \n(Pooled)", "Flow Tracing \n(Direct)", "Flow Tracing \n(Aggregated)"]

export_method_types = ["CFT", "Netted CFT", "Net Exports CFT \n(Pooled)",
                       "Net Exports Phys. \n(Pooled)", "Flow Tracing \n(Direct)", "Flow Tracing \n(Aggregated)"]

#%%
import_method_subset_types = ["Netted CFT", "Net Imports CFT \n(Pooled)",
                       "Flow Tracing \n(Direct)", "Flow Tracing \n(Aggregated)"]

export_method_subset_types = ["Netted CFT", "Net Exports CFT \n(Pooled)",
                       "Flow Tracing \n(Direct)", "Flow Tracing \n(Aggregated)"]
#%%
method_subset_types = ["Netted CFT", "Pooled Net CFT",
                       "DC Flow Tracing", "AC Flow Tracing"]
#%%
total_imports_overall = pd.DataFrame(index=bidding_zones, columns=["Total Import"])

for country in bidding_zones:
    total_imports_overall.at[country, "Total Import"] = import_per_bidding_zone_methods[country].sum().sum()

total_imports_overall = total_imports_overall.sort_values(by="Total Import", ascending=False)
#%%
total_exports_overall = pd.DataFrame(index=bidding_zones, columns=["Total Export"])

for country in bidding_zones:
    total_exports_overall.at[country, "Total Export"] = export_per_bidding_zone_methods[country].sum().sum()

total_exports_overall = total_exports_overall.sort_values(by="Total Export", ascending=False)
#%% 2025
main_importers = ["DE_LU", "IT_NORD", "GB", "CH", "SE_4", "HU"]
main_exporters = ["FR", "SE_2", "DE_LU", "NO_2", "CH", "NL"]
#%% 2024
#main_importers = ["DE_LU", "GB", "IT_NORD", "NO_1", "SE_4", "HU"]
#main_exporters = ["FR", "SE_2", "NO_2", "CH", "DE_LU", "ES"]
#%% 2022
#main_importers = ["DE_LU", "FR", "IT_NORD", "NO_1", "SE_4", "GB"]
#main_exporters = ["FR", "SE_2", "NO_2", "CH", "DE_LU", "GB"]
#%%
top_exporters = []

for country in main_importers:
    #2022
    #for x in import_per_bidding_zone_methods[country].sum().sort_values(ascending=False).index[:4]:
    for x in import_per_bidding_zone_methods[country].sum().sort_values(ascending=False).index[:5]:
        if x not in top_exporters:
            top_exporters.append(x)
            
for country in main_exporters:
    if country not in top_exporters:
        top_exporters.append(country)

#2025
top_exporters.remove("RS")

#2024
#top_exporters.remove("CZ")
#%%
top_importers = []

for country in main_exporters:
    #2025 or 2022
    for x in export_per_bidding_zone_methods[country].sum().sort_values(ascending=False).index[:5]:
    # 2024
    #for x in export_per_bidding_zone_methods[country].sum().sort_values(ascending=False).index[:4]:
        if x not in top_importers:
            top_importers.append(x)
            
for country in main_importers:
    if country not in top_importers:
        top_importers.append(country)

# 2025
top_importers.remove("HU")
top_importers.remove("IT_CSUD")

#2024
# top_importers.remove("PL")
# top_importers.remove("NO_3")
# top_importers.remove("CZ")
# top_importers.append("ES")
# top_importers.append("CH")
#%%
main_importers_per_method = {}

color_mapping = dict(zip(method_subset_types, list(colors.TABLEAU_COLORS.keys())[6:10]))

for m_type in method_subset_types:
    main_importers_per_method[m_type] = []
    for country in main_importers:
        main_importers_per_method[m_type].append(import_per_bidding_zone_methods[country].sum(axis=1)[m_type])
#%%
annual_totals_main_importers_dir = os.path.join(outputs_dir, "annual_totals_per_method/{}/main_importers/Plots".format(range_start.strftime('%Y')))

if not os.path.exists(annual_totals_main_importers_dir):
    os.makedirs(annual_totals_main_importers_dir)
    
x = np.arange(len(main_importers))  # the label locations
width = 0.2  # the width of the bars
multiplier = 0

fig, ax = plt.subplots(figsize = (18, 10))

for m_type in method_subset_types:
    offset = width * multiplier
    rects = ax.bar(x + offset, np.around(np.array(main_importers_per_method[m_type]), 2), 
                   edgecolor = "black", width=width, color=color_mapping[m_type], label=m_type)
    ax.bar_label(rects, padding=3, fontsize=11)
    multiplier += 1

# Add some text for labels, title and custom x-axis tick labels, etc.
for label in (ax.get_xticklabels() + ax.get_yticklabels()): 
    label.set_fontsize(16)
    
ax.set_ylabel('TWh', fontsize=18)
#ax.set_title('DE Import & Export Totals - {}', fontsize=18)
ax.set_xticks(x + 1.5*width, main_importers, fontsize=18)
plt.xticks(rotation=0)
ax.legend(loc='upper right', fontsize=18)
ax.set_axisbelow(True)
ax.yaxis.grid(color='silver', linestyle='dashed')
plt.ylim(top=70)
plt.yticks(np.arange(0, 70, 5), fontsize=18)
#fig.legend(ncols=5, fontsize=12)
#ax.set_ylim(0, 250)
fig.savefig(os.path.join(annual_totals_main_importers_dir, "Main Importers Import Totals {}.png".format(range_start.strftime('%Y'))), dpi = 200, bbox_inches="tight")
plt.close()
#%%
main_importers_per_method_per_bidding_zone = {}
labels = bidding_zones.copy()

colorss = list(colors.TABLEAU_COLORS.keys()) + ["salmon", "maroon", "darkmagenta", "white", "wheat"]
color_mapping = dict(zip(top_exporters, colorss))
color_mapping["Other"] = "lightgrey"
#%%
for m_type in method_subset_types:
    main_importers_per_method_per_bidding_zone[m_type] = {}
    top_exporters_contributions = np.zeros(len(main_importers))
    for label in top_exporters:
        main_importers_per_method_per_bidding_zone[m_type][label] = []
        count=0
        for country in main_importers:
            if label != country:
                main_importers_per_method_per_bidding_zone[m_type][label].append(import_per_bidding_zone_methods[country].at[m_type, label])
                top_exporters_contributions[count] += import_per_bidding_zone_methods[country].at[m_type, label]
                count+=1
            else:
                main_importers_per_method_per_bidding_zone[m_type][label].append(0.0)
                count+=1
    
    main_importers_per_method_per_bidding_zone[m_type]["Other"] = []
    count=0
    for country in main_importers:
        main_importers_per_method_per_bidding_zone[m_type]["Other"].append(import_per_bidding_zone_methods[country].sum(axis=1)[m_type] - top_exporters_contributions[count])
        count+=1
#%%
x = np.arange(len(main_importers))     
width = 0.2  # the width of the bars
multiplier = 0

fig, ax = plt.subplots(figsize = (18, 10), tight_layout=True)
bottom = np.zeros(len(main_importers))

offset = width/2

for m_type in method_subset_types:
    offset = width * multiplier
    for label in top_exporters+["Other"]:
        rects = ax.bar(x + offset, np.around(np.array(main_importers_per_method_per_bidding_zone[m_type][label]), 2), width=width, 
                       label=label, color=color_mapping[label], edgecolor = "black", bottom=bottom)
        bottom += np.around(np.array(main_importers_per_method_per_bidding_zone[m_type][label]), 2)
    ax.bar_label(rects, padding=3, fontsize=12)
    bottom = np.zeros(len(main_importers))
    multiplier += 1

ax.set_ylabel('TWh', fontsize=18)
#ax.set_title('DE Import & Export Totals - {}', fontsize=18)
main_importers_label = ["\n\n\n\n\n{}".format(main_importers[0]), "\n\n\n\n\n{}".format(main_importers[1]), "\n\n\n\n\n{}".format(main_importers[2]), 
                        "\n\n\n\n\n{}".format(main_importers[3]), "\n\n\n\n\n{}".format(main_importers[4]), "\n\n\n\n\n{}".format(main_importers[5])]
x_tick_locs = np.array([0, 0.2, 0.4, 0.6,
                1, 1.2, 1.4, 1.6,
                2, 2.2, 2.4, 2.6,
                3, 3.2, 3.4, 3.6,
                4, 4.2, 4.4, 4.6,
                5, 5.2, 5.4, 5.6])

x_tick_labels = ["Netted CFT", "P. Net CFT", "DC FT", "AC FT"]*6
ax.set_xticks(x + 1.5*width, main_importers_label, fontsize=18)
#plt.xticks(rotation=0)

sec = ax.secondary_xaxis(location=0)
sec.set_xticks(x_tick_locs, labels=x_tick_labels, rotation=90, fontsize=14)

handles, labels = plt.gca().get_legend_handles_labels()
newLabels, newHandles = [], []
for handle, label in zip(handles, labels):
  if label not in newLabels:
    newLabels.append(label)
    newHandles.append(handle)
ax.legend(newHandles, newLabels, loc='upper right', ncols=4, fontsize=18)
ax.set_axisbelow(True)
ax.yaxis.grid(color='silver', linestyle='dashed')
plt.ylim(top=70)
plt.yticks(np.arange(0, 70, 5), fontsize=18)
#fig.legend(labels=labels, ncols=10, fontsize=12)
#ax.set_ylim(0, 250)
fig.savefig(os.path.join(annual_totals_main_importers_dir, "Main Importers Imports per Country {}.png".format(range_start.strftime('%Y'))), dpi = 200, bbox_inches="tight")
plt.close()
#%%
import_per_bidding_zone_methods_percent = {}

for country in main_importers:
    import_per_bidding_zone_methods_percent[country] = pd.DataFrame(index=method_subset_types, columns=import_per_bidding_zone_methods[country].columns)
    for m_type in method_subset_types:
        import_per_bidding_zone_methods_percent[country].loc[m_type] = 100 * import_per_bidding_zone_methods[country].loc[m_type] / import_per_bidding_zone_methods[country].loc[m_type].sum()
#%%
for m_type in method_subset_types:
    main_importers_per_method_per_bidding_zone[m_type] = {}
    top_exporters_contributions = np.zeros(len(main_importers))
    for label in top_exporters:
        main_importers_per_method_per_bidding_zone[m_type][label] = []
        count=0
        for country in main_importers:
            if label != country:
                main_importers_per_method_per_bidding_zone[m_type][label].append(import_per_bidding_zone_methods_percent[country].at[m_type, label])
                top_exporters_contributions[count] += import_per_bidding_zone_methods_percent[country].at[m_type, label]
                count+=1
            else:
                main_importers_per_method_per_bidding_zone[m_type][label].append(0.0)
                count+=1
    
    main_importers_per_method_per_bidding_zone[m_type]["Other"] = []
    count=0
    for country in main_importers:
        main_importers_per_method_per_bidding_zone[m_type]["Other"].append(import_per_bidding_zone_methods_percent[country].sum(axis=1)[m_type] - top_exporters_contributions[count])
        count+=1
#%%
x = np.arange(len(main_importers))     
width = 0.2  # the width of the bars
multiplier = 0

fig, ax = plt.subplots(figsize = (18, 10), tight_layout=True)
bottom = np.zeros(len(main_importers))

labels = bidding_zones.copy()

offset = width/2

for m_type in method_subset_types:
    offset = width * multiplier
    for label in top_exporters+["Other"]:
        rects = ax.bar(x + offset, np.around(np.array(main_importers_per_method_per_bidding_zone[m_type][label]), 2), width=width, 
                       label=label, color=color_mapping[label], edgecolor = "black", bottom=bottom)
        bottom += np.around(np.array(main_importers_per_method_per_bidding_zone[m_type][label]), 2)
    #ax.bar_label(rects, padding=3)
    bottom = np.zeros(len(main_importers))
    multiplier += 1

ax.set_ylabel('%', fontsize=18)
#ax.set_title('DE Import & Export Totals - {}', fontsize=18)

main_importers_label = ["\n\n\n\n\n{}".format(main_importers[0]), "\n\n\n\n\n{}".format(main_importers[1]), "\n\n\n\n\n{}".format(main_importers[2]), 
                        "\n\n\n\n\n{}".format(main_importers[3]), "\n\n\n\n\n{}".format(main_importers[4]), "\n\n\n\n\n{}".format(main_importers[5])]
x_tick_locs = np.array([0, 0.2, 0.4, 0.6,
                1, 1.2, 1.4, 1.6,
                2, 2.2, 2.4, 2.6,
                3, 3.2, 3.4, 3.6,
                4, 4.2, 4.4, 4.6,
                5, 5.2, 5.4, 5.6])

x_tick_labels = ["Netted CFT", "P. Net CFT", "DC FT", "AC FT"]*6
ax.set_xticks(x + 1.5*width, main_importers_label, fontsize=18)
#plt.xticks(rotation=0)

sec = ax.secondary_xaxis(location=0)
sec.set_xticks(x_tick_locs, labels=x_tick_labels, rotation=90, fontsize=14)
handles, labels = plt.gca().get_legend_handles_labels()
newLabels, newHandles = [], []
for handle, label in zip(handles, labels):
  if label not in newLabels:
    newLabels.append(label)
    newHandles.append(handle)
    
#lgd = fig.legend(newHandles, newLabels, loc='outside upper right', ncols=7, fontsize=15)
#ax.legend(newHandles, newLabels, loc='upper right', ncols=5, fontsize=12)
#ax.legend(loc='upper right', ncols=10, fontsize=12)
ax.set_axisbelow(True)
ax.yaxis.grid(color='silver', linestyle='dashed')
plt.ylim(top=100)
plt.yticks(np.arange(0, 100, 5), fontsize=18)
#fig.legend(newHandles, newLabels, ncols=7, fontsize=15, loc='outside upper right')
#ax.set_ylim(0, 250)
fig.savefig(os.path.join(annual_totals_main_importers_dir, "Main Importers Imports per Country (%) {}.png".format(range_start.strftime('%Y'))), dpi = 200, bbox_inches="tight")
plt.close()
#%%
agg_gen_types = gen_types_df.groupby(['converted']).apply(lambda x: x['entsoe'].tolist()).to_dict()
#%%
main_importers_per_method_per_type = {}
#labels = bidding_zones.copy()

colorss = list(colors.TABLEAU_COLORS.keys()) + ["thistle", "wheat"]
color_mapping = dict(zip(agg_gen_types.keys(), colorss))
color_mapping["Solar"] = "yellow"
color_mapping["Biomass"] = "tab:green"
color_mapping["Hydro"] = "tab:blue"
color_mapping["Hard coal"] = "black"
color_mapping["Lignite"] = "tab:brown"
color_mapping["Nuclear"] = "tab:red"
color_mapping["Storage"] = "white"
#%%
for m_type in method_subset_types:
    main_importers_per_method_per_type[m_type] = {}
    for label in agg_gen_types.keys():
        main_importers_per_method_per_type[m_type][label] = []
        for country in main_importers:
            main_importers_per_method_per_type[m_type][label].append(import_per_agg_type_methods[country].at[m_type, label])

#%%
x = np.arange(len(main_importers))     
width = 0.2  # the width of the bars
multiplier = 0

fig, ax = plt.subplots(figsize = (18, 10), tight_layout=True)
bottom = np.zeros(len(main_importers))

offset = width/2

for m_type in method_subset_types:
    offset = width * multiplier
    for label in agg_gen_types.keys():
        rects = ax.bar(x + offset, np.around(np.array(main_importers_per_method_per_type[m_type][label]), 2), width=width, 
                       label=label, color=color_mapping[label], edgecolor = "black", bottom=bottom)
        bottom += np.around(np.array(main_importers_per_method_per_type[m_type][label]), 2)
    ax.bar_label(rects, padding=3, fontsize=12)
    bottom = np.zeros(len(main_importers))
    multiplier += 1

#ax.set_title('DE Import & Export Totals - {}', fontsize=18)

main_importers_label = ["\n\n\n\n\n{}".format(main_importers[0]), "\n\n\n\n\n{}".format(main_importers[1]), "\n\n\n\n\n{}".format(main_importers[2]), 
                        "\n\n\n\n\n{}".format(main_importers[3]), "\n\n\n\n\n{}".format(main_importers[4]), "\n\n\n\n\n{}".format(main_importers[5])]
x_tick_locs = np.array([0, 0.2, 0.4, 0.6,
                1, 1.2, 1.4, 1.6,
                2, 2.2, 2.4, 2.6,
                3, 3.2, 3.4, 3.6,
                4, 4.2, 4.4, 4.6,
                5, 5.2, 5.4, 5.6])

x_tick_labels = ["Netted CFT", "P. Net CFT", "DC FT", "AC FT"]*6
ax.set_xticks(x + 1.5*width, main_importers_label, fontsize=18)

sec = ax.secondary_xaxis(location=0)
sec.set_xticks(x_tick_locs, labels=x_tick_labels, rotation=90, fontsize=14)

ax.set_ylabel('TWh', fontsize=18)
handles, labels = plt.gca().get_legend_handles_labels()
newLabels, newHandles = [], []
for handle, label in zip(handles, labels):
  if label not in newLabels:
    newLabels.append(label)
    newHandles.append(handle)
ax.legend(newHandles, newLabels, loc='upper right', ncols=4, fontsize=18)
#ax.legend(loc='upper right', ncols=10, fontsize=12)
ax.set_axisbelow(True)
ax.yaxis.grid(color='silver', linestyle='dashed')
plt.ylim(top=70)
plt.yticks(np.arange(0, 70, 5), fontsize=18)
#fig.legend(labels=labels, ncols=10, fontsize=12)
#ax.set_ylim(0, 250)
fig.savefig(os.path.join(annual_totals_main_importers_dir, "Main Importers Imports per Type {}.png".format(range_start.strftime('%Y'))), dpi = 200, bbox_inches="tight")
plt.close()
#%%
import_per_agg_type_methods_percent = {}

for country in main_importers:
    import_per_agg_type_methods_percent[country] = pd.DataFrame(index=method_subset_types, columns=import_per_agg_type_methods[country].columns)
    for m_type in method_subset_types:
        import_per_agg_type_methods_percent[country].loc[m_type] = 100 * import_per_agg_type_methods[country].loc[m_type] / import_per_agg_type_methods[country].loc[m_type].sum()
#%%
main_importers_per_method_per_type_percent = {}

for m_type in method_subset_types:
    main_importers_per_method_per_type_percent[m_type] = {}
    for label in agg_gen_types.keys():
        main_importers_per_method_per_type_percent[m_type][label] = []
        for country in main_importers:
            main_importers_per_method_per_type_percent[m_type][label].append(import_per_agg_type_methods_percent[country].at[m_type, label])

#%%
x = np.arange(len(main_importers))     
width = 0.2  # the width of the bars
multiplier = 0

fig, ax = plt.subplots(figsize = (18, 10), tight_layout=True)
bottom = np.zeros(len(main_importers))

labels = bidding_zones.copy()

offset = width/2

for m_type in method_subset_types:
    offset = width * multiplier
    for label in agg_gen_types.keys():
        rects = ax.bar(x + offset, np.around(np.array(main_importers_per_method_per_type_percent[m_type][label]), 2), width=width, 
                       label=label, color=color_mapping[label], edgecolor = "black", bottom=bottom)
        bottom += np.around(np.array(main_importers_per_method_per_type_percent[m_type][label]), 2)
    #ax.bar_label(rects, padding=3)
    bottom = np.zeros(len(main_importers))
    multiplier += 1

ax.set_ylabel('%', fontsize=18)
#ax.set_title('DE Import & Export Totals - {}', fontsize=18)

main_importers_label = ["\n\n\n\n\n{}".format(main_importers[0]), "\n\n\n\n\n{}".format(main_importers[1]), "\n\n\n\n\n{}".format(main_importers[2]), 
                        "\n\n\n\n\n{}".format(main_importers[3]), "\n\n\n\n\n{}".format(main_importers[4]), "\n\n\n\n\n{}".format(main_importers[5])]
x_tick_locs = np.array([0, 0.2, 0.4, 0.6,
                1, 1.2, 1.4, 1.6,
                2, 2.2, 2.4, 2.6,
                3, 3.2, 3.4, 3.6,
                4, 4.2, 4.4, 4.6,
                5, 5.2, 5.4, 5.6])

x_tick_labels = ["Netted CFT", "P. Net CFT", "DC FT", "AC FT"]*6
ax.set_xticks(x + 1.5*width, main_importers_label, fontsize=18)
#plt.xticks(rotation=0)

sec = ax.secondary_xaxis(location=0)
sec.set_xticks(x_tick_locs, labels=x_tick_labels, rotation=90, fontsize=14)
handles, labels = plt.gca().get_legend_handles_labels()
newLabels, newHandles = [], []
for handle, label in zip(handles, labels):
  if label not in newLabels:
    newLabels.append(label)
    newHandles.append(handle)
    
#lgd = fig.legend(newHandles, newLabels, loc='outside upper right', ncols=7, fontsize=15)
#ax.legend(newHandles, newLabels, loc='upper right', ncols=5, fontsize=12)
#ax.legend(loc='upper right', ncols=10, fontsize=12)
ax.set_axisbelow(True)
ax.yaxis.grid(color='silver', linestyle='dashed')
plt.ylim(top=100)
plt.yticks(np.arange(0, 100, 5), fontsize=18)
#fig.legend(newHandles, newLabels, ncols=7, fontsize=15, loc='outside upper right')
#ax.set_ylim(0, 250)
fig.savefig(os.path.join(annual_totals_main_importers_dir, "Main Importers Imports per Type (%) {}.png".format(range_start.strftime('%Y'))), dpi = 200, bbox_inches="tight")
plt.close()
#%%
main_exporters_per_method = {}

color_mapping = dict(zip(method_subset_types, list(colors.TABLEAU_COLORS.keys())[6:10]))

for m_type in method_subset_types:
    main_exporters_per_method[m_type] = []
    for country in main_exporters:
        main_exporters_per_method[m_type].append(export_per_bidding_zone_methods[country].sum(axis=1)[m_type])
#%%
annual_totals_main_exporters_dir = os.path.join(outputs_dir, "annual_totals_per_method/{}/main_exporters/Plots".format(range_start.strftime('%Y')))
#%%
if not os.path.exists(annual_totals_main_exporters_dir):
    os.makedirs(annual_totals_main_exporters_dir)
    
x = np.arange(len(main_exporters))  # the label locations
width = 0.2  # the width of the bars
multiplier = 0

fig, ax = plt.subplots(figsize = (18, 10))

for m_type in method_subset_types:
    offset = width * multiplier
    rects = ax.bar(x + offset, np.around(np.array(main_exporters_per_method[m_type]), 2), 
                   width=width, color=color_mapping[m_type], edgecolor = "black", label=m_type)
    ax.bar_label(rects, padding=3, fontsize=11)
    multiplier += 1

# Add some text for labels, title and custom x-axis tick labels, etc.
for label in (ax.get_xticklabels() + ax.get_yticklabels()): 
    label.set_fontsize(18)
    
ax.set_ylabel('TWh', fontsize=18)
#ax.set_title('DE Import & Export Totals - {}', fontsize=18)
ax.set_xticks(x + 1.5*width, main_exporters)
plt.xticks(rotation=0)
ax.legend(loc='upper right', fontsize=18)
ax.set_axisbelow(True)
ax.yaxis.grid(color='silver', linestyle='dashed')
plt.ylim(top=105)
plt.yticks(np.arange(0, 105, 5), fontsize=18)
#fig.legend(ncols=5, fontsize=12)
#ax.set_ylim(0, 250)
fig.savefig(os.path.join(annual_totals_main_exporters_dir, "Main Exporters Export Totals {}.png".format(range_start.strftime('%Y'))), dpi = 200, bbox_inches="tight")
plt.close()  
#%%
main_exporters_per_method_per_bidding_zone = {}
labels = bidding_zones.copy()

colorss = list(reversed(list(colors.TABLEAU_COLORS.keys()))) + ["salmon", "maroon", "darkmagenta", "white", "wheat"]
color_mapping = dict(zip(top_importers, colorss))
color_mapping["Other"] = "lightgrey"
#%%
for m_type in method_subset_types:
    main_exporters_per_method_per_bidding_zone[m_type] = {}
    top_importers_contributions = np.zeros(len(main_exporters))
    for label in top_importers:
        main_exporters_per_method_per_bidding_zone[m_type][label] = []
        count=0
        for country in main_exporters:
            if label != country:
                main_exporters_per_method_per_bidding_zone[m_type][label].append(export_per_bidding_zone_methods[country].at[m_type, label])
                top_importers_contributions[count] += export_per_bidding_zone_methods[country].at[m_type, label]
                count+=1
            else:
                main_exporters_per_method_per_bidding_zone[m_type][label].append(0.0)
                count+=1
    
    main_exporters_per_method_per_bidding_zone[m_type]["Other"] = []
    count=0
    for country in main_exporters:
        main_exporters_per_method_per_bidding_zone[m_type]["Other"].append(export_per_bidding_zone_methods[country].sum(axis=1)[m_type] - top_importers_contributions[count])
        count+=1    
#%%
x = np.arange(len(main_exporters))     
width = 0.2  # the width of the bars
multiplier = 0

fig, ax = plt.subplots(figsize = (18, 10), tight_layout=True)
bottom = np.zeros(len(main_exporters))

offset = width/2

for m_type in method_subset_types:
    offset = width * multiplier
    for label in top_importers+["Other"]:
        rects = ax.bar(x + offset, np.around(np.array(main_exporters_per_method_per_bidding_zone[m_type][label]), 2), width=width, 
                       label=label, color=color_mapping[label], edgecolor = "black", bottom=bottom)
        bottom += np.around(np.array(main_exporters_per_method_per_bidding_zone[m_type][label]), 2)
    ax.bar_label(rects, padding=3, fontsize=12)
    bottom = np.zeros(len(main_exporters))
    multiplier += 1

ax.set_ylabel('TWh', fontsize=18)
#ax.set_title('DE Import & Export Totals - {}', fontsize=18)

main_exporters_label = ["\n\n\n\n\n{}".format(main_exporters[0]), "\n\n\n\n\n{}".format(main_exporters[1]), "\n\n\n\n\n{}".format(main_exporters[2]), 
                        "\n\n\n\n\n{}".format(main_exporters[3]), "\n\n\n\n\n{}".format(main_exporters[4]), "\n\n\n\n\n{}".format(main_exporters[5])]
x_tick_locs = np.array([0, 0.2, 0.4, 0.6,
                1, 1.2, 1.4, 1.6,
                2, 2.2, 2.4, 2.6,
                3, 3.2, 3.4, 3.6,
                4, 4.2, 4.4, 4.6,
                5, 5.2, 5.4, 5.6])

x_tick_labels = ["Netted CFT", "P. Net CFT", "DC FT", "AC FT"]*6
ax.set_xticks(x + 1.5*width, main_exporters_label, fontsize=18)
#plt.xticks(rotation=0)

sec = ax.secondary_xaxis(location=0)
sec.set_xticks(x_tick_locs, labels=x_tick_labels, rotation=90, fontsize=14)
handles, labels = plt.gca().get_legend_handles_labels()
newLabels, newHandles = [], []
for handle, label in zip(handles, labels):
  if label not in newLabels:
    newLabels.append(label)
    newHandles.append(handle)
ax.legend(newHandles, newLabels, loc='upper right', ncols=4, fontsize=18)
#ax.legend(loc='upper right', ncols=10, fontsize=12)
ax.set_axisbelow(True)
ax.yaxis.grid(color='silver', linestyle='dashed')
plt.ylim(top=105)
plt.yticks(np.arange(0, 105, 5), fontsize=18)
#fig.legend(labels=labels, ncols=10, fontsize=12)
#ax.set_ylim(0, 250)
fig.savefig(os.path.join(annual_totals_main_exporters_dir, "Main Exporters Exports per Country {}.png".format(range_start.strftime('%Y'))), dpi = 200, bbox_inches="tight")
plt.close()
#%%
export_per_bidding_zone_methods_percent = {}
main_exporters_per_method_per_bidding_zone_percent = {}

for country in main_exporters:
    export_per_bidding_zone_methods_percent[country] = pd.DataFrame(index=method_subset_types, columns=export_per_bidding_zone_methods[country].columns)
    for m_type in method_subset_types:
        export_per_bidding_zone_methods_percent[country].loc[m_type] = 100 * export_per_bidding_zone_methods[country].loc[m_type] / export_per_bidding_zone_methods[country].loc[m_type].sum()
#%%
for m_type in method_subset_types:
    main_exporters_per_method_per_bidding_zone_percent[m_type] = {}
    top_importers_contributions = np.zeros(len(main_exporters))
    for label in top_importers:
        main_exporters_per_method_per_bidding_zone_percent[m_type][label] = []
        count=0
        for country in main_exporters:
            if label != country:
                main_exporters_per_method_per_bidding_zone_percent[m_type][label].append(export_per_bidding_zone_methods_percent[country].at[m_type, label])
                top_importers_contributions[count] += export_per_bidding_zone_methods_percent[country].at[m_type, label]
                count+=1
            else:
                main_exporters_per_method_per_bidding_zone_percent[m_type][label].append(0.0)
                count+=1
    
    main_exporters_per_method_per_bidding_zone_percent[m_type]["Other"] = []
    count=0
    for country in main_exporters:
        main_exporters_per_method_per_bidding_zone_percent[m_type]["Other"].append(export_per_bidding_zone_methods_percent[country].sum(axis=1)[m_type] - top_importers_contributions[count])
        count+=1    
#%%
x = np.arange(len(main_exporters))     
width = 0.2  # the width of the bars
multiplier = 0

fig, ax = plt.subplots(figsize = (18, 10), tight_layout=True)
bottom = np.zeros(len(main_exporters))

labels = bidding_zones.copy()

offset = width/2

for m_type in method_subset_types:
    offset = width * multiplier
    for label in top_importers+["Other"]:
        rects = ax.bar(x + offset, np.around(np.array(main_exporters_per_method_per_bidding_zone_percent[m_type][label]), 2), width=width, 
                       label=label, color=color_mapping[label], edgecolor = "black", bottom=bottom)
        bottom += np.around(np.array(main_exporters_per_method_per_bidding_zone_percent[m_type][label]), 2)
    #ax.bar_label(rects, padding=3)
    bottom = np.zeros(len(main_exporters))
    multiplier += 1

ax.set_ylabel('%', fontsize=18)
#ax.set_title('DE Import & Export Totals - {}', fontsize=18)

main_exporters_label = ["\n\n\n\n\n{}".format(main_exporters[0]), "\n\n\n\n\n{}".format(main_exporters[1]), "\n\n\n\n\n{}".format(main_exporters[2]), 
                        "\n\n\n\n\n{}".format(main_exporters[3]), "\n\n\n\n\n{}".format(main_exporters[4]), "\n\n\n\n\n{}".format(main_exporters[5])]
x_tick_locs = np.array([0, 0.2, 0.4, 0.6,
                1, 1.2, 1.4, 1.6,
                2, 2.2, 2.4, 2.6,
                3, 3.2, 3.4, 3.6,
                4, 4.2, 4.4, 4.6,
                5, 5.2, 5.4, 5.6])

x_tick_labels = ["Netted CFT", "P. Net CFT", "DC FT", "AC FT"]*6
ax.set_xticks(x + 1.5*width, main_exporters_label, fontsize=18)
#plt.xticks(rotation=0)

sec = ax.secondary_xaxis(location=0)
sec.set_xticks(x_tick_locs, labels=x_tick_labels, rotation=90, fontsize=14)
handles, labels = plt.gca().get_legend_handles_labels()
newLabels, newHandles = [], []
for handle, label in zip(handles, labels):
  if label not in newLabels:
    newLabels.append(label)
    newHandles.append(handle)
    
#lgd = fig.legend(newHandles, newLabels, loc='outside upper right', ncols=7, fontsize=15)
#ax.legend(newHandles, newLabels, loc='upper right', ncols=5, fontsize=12)
#ax.legend(loc='upper right', ncols=10, fontsize=12)
ax.set_axisbelow(True)
ax.yaxis.grid(color='silver', linestyle='dashed')
plt.ylim(top=100)
plt.yticks(np.arange(0, 100, 5), fontsize=18)
#fig.legend(newHandles, newLabels, ncols=7, fontsize=15, loc='outside upper right')
#ax.set_ylim(0, 250)
fig.savefig(os.path.join(annual_totals_main_exporters_dir, "Main Exporters Exports per Country (%) {}.png".format(range_start.strftime('%Y'))), dpi = 200, bbox_inches="tight")
plt.close()
#%%
export_per_agg_type_methods = {}
main_exporters_per_method_per_type = {}

for country in main_exporters:
    export_per_agg_type_methods[country] = pd.DataFrame(index=method_subset_types, columns=agg_gen_types.keys())
    for m_type in method_subset_types:
        for agg_type, types in agg_gen_types.items():
            present_types = [export_per_type_methods[country].at[m_type, x] for x in types if x in export_per_type_methods[country].columns] 
            export_per_agg_type_methods[country].at[m_type, agg_type] = sum(present_types)
#%%
colorss = list(colors.TABLEAU_COLORS.keys()) + ["thistle", "wheat"]
color_mapping = dict(zip(agg_gen_types.keys(), colorss))
color_mapping["Solar"] = "yellow"
color_mapping["Biomass"] = "tab:green"
color_mapping["Hydro"] = "tab:blue"
color_mapping["Hard coal"] = "black"
color_mapping["Lignite"] = "tab:brown"
color_mapping["Nuclear"] = "tab:red"
color_mapping["Storage"] = "white"
#labels = bidding_zones.copy()
#%%
for m_type in method_subset_types:
    main_exporters_per_method_per_type[m_type] = {}
    for label in agg_gen_types.keys():
        main_exporters_per_method_per_type[m_type][label] = []
        for country in main_exporters:
            main_exporters_per_method_per_type[m_type][label].append(export_per_agg_type_methods[country].at[m_type, label])

#%%
x = np.arange(len(main_exporters))     
width = 0.2  # the width of the bars
multiplier = 0

fig, ax = plt.subplots(figsize = (18, 10), tight_layout=True)
bottom = np.zeros(len(main_exporters))

offset = width/2

for m_type in method_subset_types:
    offset = width * multiplier
    for label in agg_gen_types.keys():
        rects = ax.bar(x + offset, np.around(np.array(main_exporters_per_method_per_type[m_type][label]), 2), width=width, 
                       label=label, color=color_mapping[label], edgecolor = "black", bottom=bottom)
        bottom += np.around(np.array(main_exporters_per_method_per_type[m_type][label]), 2)
    ax.bar_label(rects, padding=3, fontsize=12)
    bottom = np.zeros(len(main_exporters))
    multiplier += 1

ax.set_ylabel('TWh', fontsize=18)
#ax.set_title('DE export & Export Totals - {}', fontsize=18)
main_exporters_label = ["\n\n\n\n\n{}".format(main_exporters[0]), "\n\n\n\n\n{}".format(main_exporters[1]), "\n\n\n\n\n{}".format(main_exporters[2]), 
                        "\n\n\n\n\n{}".format(main_exporters[3]), "\n\n\n\n\n{}".format(main_exporters[4]), "\n\n\n\n\n{}".format(main_exporters[5])]
x_tick_locs = np.array([0, 0.2, 0.4, 0.6,
                1, 1.2, 1.4, 1.6,
                2, 2.2, 2.4, 2.6,
                3, 3.2, 3.4, 3.6,
                4, 4.2, 4.4, 4.6,
                5, 5.2, 5.4, 5.6])

x_tick_labels = ["Netted CFT", "P. Net CFT", "DC FT", "AC FT"]*6
ax.set_xticks(x + 1.5*width, main_exporters_label, fontsize=18)
#plt.xticks(rotation=0)

sec = ax.secondary_xaxis(location=0)
sec.set_xticks(x_tick_locs, labels=x_tick_labels, rotation=90, fontsize=14)

handles, labels = plt.gca().get_legend_handles_labels()
newLabels, newHandles = [], []
for handle, label in zip(handles, labels):
  if label not in newLabels:
    newLabels.append(label)
    newHandles.append(handle)
ax.legend(newHandles, newLabels, loc='upper right', ncols=4, fontsize=18)
#ax.legend(loc='upper right', ncols=10, fontsize=12)
ax.set_axisbelow(True)
ax.yaxis.grid(color='silver', linestyle='dashed')
plt.ylim(top=105)
plt.yticks(np.arange(0, 105, 5), fontsize=18)
#fig.legend(newHandles, newLabels, ncols=6, fontsize=15)
#ax.set_ylim(0, 250)
fig.savefig(os.path.join(annual_totals_main_exporters_dir, "Main Exporters Exports per Type {}.png".format(range_start.strftime('%Y'))), dpi = 200, bbox_inches="tight")
plt.close()
#%%
export_per_agg_type_methods_percent = {}
main_exporters_per_method_per_type_percent = {}

for country in main_exporters:
    export_per_agg_type_methods_percent[country] = pd.DataFrame(index=method_subset_types, columns=export_per_agg_type_methods[country].columns)
    for m_type in method_subset_types:
        export_per_agg_type_methods_percent[country].loc[m_type] = 100 * export_per_agg_type_methods[country].loc[m_type] / export_per_agg_type_methods[country].loc[m_type].sum()
#%%
for m_type in method_subset_types:
    main_exporters_per_method_per_type_percent[m_type] = {}
    for label in agg_gen_types.keys():
        main_exporters_per_method_per_type_percent[m_type][label] = []
        for country in main_exporters:
            main_exporters_per_method_per_type_percent[m_type][label].append(export_per_agg_type_methods_percent[country].at[m_type, label])

#%%
x = np.arange(len(main_exporters))     
width = 0.2  # the width of the bars
multiplier = 0

fig, ax = plt.subplots(figsize = (18, 10), tight_layout=True)
bottom = np.zeros(len(main_exporters))

labels = bidding_zones.copy()

offset = width/2

for m_type in method_subset_types:
    offset = width * multiplier
    for label in agg_gen_types.keys():
        rects = ax.bar(x + offset, np.around(np.array(main_exporters_per_method_per_type_percent[m_type][label]), 2), width=width, 
                       label=label, color=color_mapping[label], edgecolor = "black", bottom=bottom)
        bottom += np.around(np.array(main_exporters_per_method_per_type_percent[m_type][label]), 2)
    #ax.bar_label(rects, padding=3)
    bottom = np.zeros(len(main_exporters))
    multiplier += 1

ax.set_ylabel('%', fontsize=18)
#ax.set_title('DE Import & Export Totals - {}', fontsize=18)

main_exporters_label = ["\n\n\n\n\n{}".format(main_exporters[0]), "\n\n\n\n\n{}".format(main_exporters[1]), "\n\n\n\n\n{}".format(main_exporters[2]), 
                        "\n\n\n\n\n{}".format(main_exporters[3]), "\n\n\n\n\n{}".format(main_exporters[4]), "\n\n\n\n\n{}".format(main_exporters[5])]
x_tick_locs = np.array([0, 0.2, 0.4, 0.6,
                1, 1.2, 1.4, 1.6,
                2, 2.2, 2.4, 2.6,
                3, 3.2, 3.4, 3.6,
                4, 4.2, 4.4, 4.6,
                5, 5.2, 5.4, 5.6])

x_tick_labels = ["Netted CFT", "P. Net CFT", "DC FT", "AC FT"]*6
ax.set_xticks(x + 1.5*width, main_exporters_label, fontsize=18)
#plt.xticks(rotation=0)

sec = ax.secondary_xaxis(location=0)
sec.set_xticks(x_tick_locs, labels=x_tick_labels, rotation=90, fontsize=14)
handles, labels = plt.gca().get_legend_handles_labels()
newLabels, newHandles = [], []
for handle, label in zip(handles, labels):
  if label not in newLabels:
    newLabels.append(label)
    newHandles.append(handle)
    
#lgd = fig.legend(newHandles, newLabels, loc='outside upper right', ncols=7, fontsize=15)
#ax.legend(newHandles, newLabels, loc='upper right', ncols=5, fontsize=12)
#ax.legend(loc='upper right', ncols=10, fontsize=12)
ax.set_axisbelow(True)
ax.yaxis.grid(color='silver', linestyle='dashed')
plt.ylim(top=100)
plt.yticks(np.arange(0, 100, 5), fontsize=18)
#fig.legend(newHandles, newLabels, ncols=7, fontsize=15, loc='outside upper right')
#ax.set_ylim(0, 250)
fig.savefig(os.path.join(annual_totals_main_exporters_dir, "Main Exporters Exports per Type (%) {}.png".format(range_start.strftime('%Y'))), dpi = 200, bbox_inches="tight")
plt.close()
#%%
#%%
#%%
annual_totals_export_per_type_plots_dir = os.path.join(annual_totals_export_per_type_dir, "Plots")

if not os.path.exists(annual_totals_export_per_type_plots_dir):
    os.makedirs(annual_totals_export_per_type_plots_dir)
    
for country in bidding_zones:    

    x = np.arange(len(export_per_type_methods[country].index))     
    width = 0.5  # the width of the bars
    multiplier = 0
    
    fig, ax = plt.subplots(figsize = (30, 20))
    bottom = np.zeros(len(export_per_type_methods[country].index))
    
    labels = list(export_per_type_methods[country].columns)
    
    offset = width/2
    
    for label in labels:
        rects = ax.bar(x + offset, np.around(np.array(export_per_type_methods[country][label]), 3), 
                       width=width, edgecolor = "black", label=label, bottom=bottom)
        bottom += np.around(np.array(export_per_type_methods[country][label]), 3)
    
    ax.bar_label(rects, padding=3)
    #multiplier += 1
    #bottom = np.zeros(len(neighbours))
    for label in (ax.get_xticklabels() + ax.get_yticklabels()): 
        label.set_fontsize(16)
    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('TWh', fontsize=16)
    plt.xticks(rotation=20)
    #ax.set_title('{} Import per Type {}'.format(country))
    ax.set_xticks(x + width/2, export_method_types)
    plt.yticks(np.arange(0, export_per_type_methods[country].sum(axis=1).max()+5, 5))
    ax.set_axisbelow(True)
    ax.yaxis.grid(color='silver', linestyle='dashed')
    fig.legend(labels=labels, ncols=6, fontsize=12)
    
    fig.savefig(os.path.join(annual_totals_export_per_type_plots_dir, "{} Export per Type {}.png".format(country, range_start.strftime('%Y'))), dpi = 200, bbox_inches="tight")
    plt.close()
    
#%%
annual_totals_import_per_type_plots_dir = os.path.join(annual_totals_import_per_type_dir, "Plots")

if not os.path.exists(annual_totals_import_per_type_plots_dir):
    os.makedirs(annual_totals_import_per_type_plots_dir)
    
for country in bidding_zones:    

    x = np.arange(len(import_per_type_methods[country].index))     
    width = 0.5  # the width of the bars
    multiplier = 0
    
    fig, ax = plt.subplots(figsize = (30, 20))
    bottom = np.zeros(len(import_per_type_methods[country].index))
    
    labels = list(import_per_type_methods[country].columns)
    
    offset = width/2
    
    for label in labels:
        rects = ax.bar(x + offset, np.around(np.array(import_per_type_methods[country][label]), 3), 
                       width=width, edgecolor = "black", label=label, bottom=bottom)
        bottom += np.around(np.array(import_per_type_methods[country][label]), 3)
    
    ax.bar_label(rects, padding=3)
    #multiplier += 1
    #bottom = np.zeros(len(neighbours))
    for label in (ax.get_xticklabels() + ax.get_yticklabels()): 
        label.set_fontsize(16)
    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('TWh', fontsize=16)
    plt.xticks(rotation=20)
    #ax.set_title('{} Import per Type {}'.format(country))
    ax.set_xticks(x + width/2, import_method_types)
    plt.yticks(np.arange(0, import_per_type_methods[country].sum(axis=1).max()+5, 5))
    ax.set_axisbelow(True)
    ax.yaxis.grid(color='silver', linestyle='dashed')
    fig.legend(labels=labels, ncols=6, fontsize=12)
    
    fig.savefig(os.path.join(annual_totals_import_per_type_plots_dir, "{} Import per Type {}.png".format(country, range_start.strftime('%Y'))), dpi = 200, bbox_inches="tight")
    plt.close()
#%%
annual_totals_import_per_agg_type_plots_dir = os.path.join(annual_totals_import_per_agg_type_dir, "Plots")

if not os.path.exists(annual_totals_import_per_agg_type_plots_dir):
    os.makedirs(annual_totals_import_per_agg_type_plots_dir)
    
for country in bidding_zones:    

    x = np.arange(len(import_per_agg_type_methods[country].index))     
    width = 0.5  # the width of the bars
    multiplier = 0
    
    fig, ax = plt.subplots(figsize = (30, 20))
    bottom = np.zeros(len(import_per_agg_type_methods[country].index))
    
    labels = list(import_per_agg_type_methods[country].columns)
    
    offset = width/2
    
    for label in labels:
        rects = ax.bar(x + offset, np.around(np.array(import_per_agg_type_methods[country][label]), 3), 
                       width=width, edgecolor = "black", label=label, bottom=bottom)
        bottom += np.around(np.array(import_per_agg_type_methods[country][label]), 3)
    
    ax.bar_label(rects, padding=3)
    #multiplier += 1
    #bottom = np.zeros(len(neighbours))
    for label in (ax.get_xticklabels() + ax.get_yticklabels()): 
        label.set_fontsize(16)
    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('TWh', fontsize=16)
    plt.xticks(rotation=20)
    #ax.set_title('{} Import per Type {}'.format(country))
    ax.set_xticks(x + width/2, import_method_types)
    plt.yticks(np.arange(0, import_per_agg_type_methods[country].sum(axis=1).max()+5, 5))
    ax.set_axisbelow(True)
    ax.yaxis.grid(color='silver', linestyle='dashed')
    fig.legend(labels=labels, ncols=6, fontsize=16)
    
    fig.savefig(os.path.join(annual_totals_import_per_agg_type_plots_dir, "{} Import per Agg. Type {}.png".format(country, range_start.strftime('%Y'))), dpi = 200, bbox_inches="tight")
    plt.close()
#%%
annual_totals_export_per_bidding_zone_plots_dir = os.path.join(annual_totals_export_per_bidding_zone_dir, "Plots")

if not os.path.exists(annual_totals_export_per_bidding_zone_plots_dir):
    os.makedirs(annual_totals_export_per_bidding_zone_plots_dir)
    
for country in bidding_zones:    

    x = np.arange(len(export_per_bidding_zone_methods[country].index))     
    width = 0.5  # the width of the bars
    multiplier = 0
    
    fig, ax = plt.subplots(figsize = (30, 20))
    bottom = np.zeros(len(export_per_bidding_zone_methods[country].index))
    
    labels = list(export_per_bidding_zone_methods[country].columns)
    
    offset = width/2
    
    for label in labels:
        rects = ax.bar(x + offset, np.around(np.array(export_per_bidding_zone_methods[country][label]), 3), 
                       width=width, edgecolor = "black", label=label, bottom=bottom)
        bottom += np.around(np.array(export_per_bidding_zone_methods[country][label]), 3)
    
    ax.bar_label(rects, padding=3)
    #multiplier += 1
    #bottom = np.zeros(len(neighbours))
    for label in (ax.get_xticklabels() + ax.get_yticklabels()): 
        label.set_fontsize(16)
    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('TWh', fontsize=16)
    plt.xticks(rotation=20)
    #ax.set_title('{} Import per Type {}'.format(country))
    ax.set_xticks(x + width/2, export_method_types)
    plt.yticks(np.arange(0, export_per_bidding_zone_methods[country].sum(axis=1).max()+5, 5))
    ax.set_axisbelow(True)
    ax.yaxis.grid(color='silver', linestyle='dashed')
    fig.legend(labels=labels, ncols=12, fontsize=12)
    
    fig.savefig(os.path.join(annual_totals_export_per_bidding_zone_plots_dir, "{} Export per Country {}.png".format(country, range_start.strftime('%Y'))), dpi = 200, bbox_inches="tight")
    plt.close()
    
#%%

annual_totals_import_per_bidding_zone_plots_dir = os.path.join(annual_totals_import_per_bidding_zone_dir, "Plots")

if not os.path.exists(annual_totals_import_per_bidding_zone_plots_dir):
    os.makedirs(annual_totals_import_per_bidding_zone_plots_dir)
    
for country in bidding_zones:    

    x = np.arange(len(import_per_bidding_zone_methods[country].index))     
    width = 0.5  # the width of the bars
    multiplier = 0
    
    fig, ax = plt.subplots(figsize = (30, 20))
    bottom = np.zeros(len(import_per_bidding_zone_methods[country].index))
    
    labels = list(import_per_bidding_zone_methods[country].columns)
    
    offset = width/2
    
    for label in labels:
        rects = ax.bar(x + offset, np.around(np.array(import_per_bidding_zone_methods[country][label]), 3), 
                       width=width, edgecolor = "black", label=label, bottom=bottom)
        bottom += np.around(np.array(import_per_bidding_zone_methods[country][label]), 3)
    
    ax.bar_label(rects, padding=3)
    #multiplier += 1
    #bottom = np.zeros(len(neighbours))
    for label in (ax.get_xticklabels() + ax.get_yticklabels()): 
        label.set_fontsize(16)
    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('TWh', fontsize=16)
    plt.xticks(rotation=20)
    #ax.set_title('{} Import per Country {}'.format(country, range_start.strftime('%Y')))
    ax.set_xticks(x + width/2, import_method_types)
    plt.yticks(np.arange(0, import_per_bidding_zone_methods[country].sum(axis=1).max()+5, 5))
    ax.set_axisbelow(True)
    ax.yaxis.grid(color='silver', linestyle='dashed')
    fig.legend(labels=labels, ncols=12, fontsize=12)
    
    fig.savefig(os.path.join(annual_totals_import_per_bidding_zone_plots_dir, "{} Import per Country {}.png".format(country, range_start.strftime('%Y'))), dpi = 200, bbox_inches="tight")
    plt.close()