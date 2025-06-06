import pandas as pd
import glob
import os
import re
import matplotlib.pyplot as plt
import seaborn as sns


LOG_DIR = "logs/"

# Path to your CSV files
csv_files = glob.glob(os.path.join(LOG_DIR, "*.csv"))  # or replace with the full path if needed

all_data = []

for file in csv_files:
    filename = os.path.basename(file)
    match = re.match(r'(.+?)_(.+?)_(.+?)_results\.csv', filename)
    
    if match:
        app, config, input_type = match.groups()
        df = pd.read_csv(file)
        df['app'] = app
        df['config'] = config
        df['input'] = input_type
        # print filename and input
        print(f"Processing file: {filename} with input type: {input_type}")
        all_data.append(df)
    else:
        print(f"Filename did not match expected pattern: {filename}")

# Combine into a single DataFrame
combined_df = pd.concat(all_data, ignore_index=True)

# Optional: Save to CSV for verification
combined_df.to_csv("combined_results.csv", index=False)

# convert input column to numeric
combined_df['input'] = combined_df['input'].str.replace('k', '').astype(int)

# select specific inputs for app==windowjoin
combined_df = combined_df[((combined_df['app'] == 'WindowJoin') & (combined_df['input'].isin([50, 100, 200, 500, 1000]))) |
                          (combined_df['app'] == 'StateMachine')]

print(combined_df[['input', 'app', 'config']].drop_duplicates())
# exit()

# sort per app and input
combined_df.sort_values(by=['app', 'input', 'config'], inplace=True)
hue_order = ['tm2x4', 'tm8x1']


fig, ax = plt.subplots(1, 2, figsize=(7, 3))
for idx, app in enumerate(combined_df['app'].unique()):
    app_data = combined_df[combined_df['app'] == app]
    if idx ==0:
        hasLegend = False
    else:
        hasLegend = True
    s = sns.barplot(
        data=app_data,
        x='input',
        y='throughput',
        hue='config',
        ax=ax[idx],
        ci=None,
        palette='husl',
        edgecolor='black',
        width=0.8, 
        hue_order=hue_order
        # legend=hasLegend,
    )
    
    if hasLegend:
        s.legend(title='Config', fontsize=7, title_fontsize=8)
    else:
        s.get_legend().remove()

    s.tick_params(axis='both', labelsize=7)
    s.set_title(app, fontsize=8)
    s.set_xlabel('In Rate', fontsize=7)
    s.set_ylabel('Out Rate', fontsize=7)

plt.subplots_adjust(
    top=0.8, 
    bottom=0.24, 
    left=0.12, 
    right=0.98, 
    hspace=0.3, 
    wspace=0.35
)
plt.savefig("plots/throughput.pdf")


fig, ax = plt.subplots(1, 2, figsize=(7, 3))
for idx, app in enumerate(combined_df['app'].unique()):
    app_data = combined_df[combined_df['app'] == app]
    if idx ==0:
        hasLegend = False
    else:
        hasLegend = True
    s = sns.barplot(
        data=app_data,
        x='input',
        y='jvm_cpu_time',
        hue='config',
        ax=ax[idx],
        ci=None,
        palette='husl',
        edgecolor='black',
        width=0.8, 
        hue_order=hue_order,
        # legend=hasLegend,
    )
    
    if hasLegend:
        s.legend(title='Config', fontsize=7, title_fontsize=8)
    else:
        s.get_legend().remove()

    s.tick_params(axis='both', labelsize=7)
    s.set_title(app, fontsize=8)
    s.set_xlabel('In Rate', fontsize=7)
    s.set_ylabel('JVM CPU time (ns)', fontsize=7)

plt.subplots_adjust(
    top=0.8, 
    bottom=0.24, 
    left=0.12, 
    right=0.98, 
    hspace=0.3, 
    wspace=0.35
)
plt.savefig("plots/jvm_cpu_time_results.pdf")

fig, ax = plt.subplots(1, 2, figsize=(7, 3))
for idx, app in enumerate(combined_df['app'].unique()):
    app_data = combined_df[combined_df['app'] == app]
    if idx ==0:
        hasLegend = False
    else:
        hasLegend = True
    s = sns.barplot(
        data=app_data,
        x='input',
        y='jvm_gc_time',
        hue='config',
        ax=ax[idx],
        ci=None,
        palette='husl',
        edgecolor='black',
        width=0.8, 
        hue_order=hue_order,
        # legend=hasLegend,
    )
    
    if hasLegend:
        s.legend(title='Config', fontsize=7, title_fontsize=8)
    else:
        s.get_legend().remove()

    s.tick_params(axis='both', labelsize=7)
    s.set_title(app, fontsize=8)
    s.set_xlabel('In Rate', fontsize=7)
    s.set_ylabel('JVM GC Time', fontsize=7)

plt.subplots_adjust(
    top=0.8, 
    bottom=0.24, 
    left=0.12, 
    right=0.98, 
    hspace=0.3, 
    wspace=0.35
)
plt.savefig("plots/jvm_gc_time_results.pdf")

fig, ax = plt.subplots(1, 2, figsize=(7, 3))
for idx, app in enumerate(combined_df['app'].unique()):
    app_data = combined_df[combined_df['app'] == app]
    if idx ==0:
        hasLegend = False
    else:
        hasLegend = True
    s = sns.barplot(
        data=app_data,
        x='input',
        y='jvm_heap_used',
        hue='config',
        ax=ax[idx],
        ci=None,
        palette='husl',
        edgecolor='black',
        width=0.8, 
        hue_order=hue_order,
        # legend=hasLegend,
    )
    
    if hasLegend:
        s.legend(title='Config', fontsize=7, title_fontsize=8)
    else:
        s.get_legend().remove()

    s.tick_params(axis='both', labelsize=7)
    s.set_title(app, fontsize=8)
    s.set_xlabel('In Rate', fontsize=7)
    s.set_ylabel('JVM Heap (bytes)', fontsize=7)

plt.subplots_adjust(
    top=0.8, 
    bottom=0.24, 
    left=0.12, 
    right=0.98, 
    hspace=0.3, 
    wspace=0.35
)
plt.savefig("plots/jvm_heap_results.pdf")

fig, ax = plt.subplots(1, 2, figsize=(7, 3))
for idx, app in enumerate(combined_df['app'].unique()):
    app_data = combined_df[combined_df['app'] == app]
    if idx ==0:
        hasLegend = False
    else:
        hasLegend = True
    s = sns.barplot(
        data=app_data,
        x='input',
        y='jvm_threads',
        hue='config',
        ax=ax[idx],
        ci=None,
        palette='husl',
        edgecolor='black',
        width=0.8, 
        hue_order=hue_order,
        # legend=hasLegend,
    )
    
    if hasLegend:
        s.legend(title='Config', fontsize=7, title_fontsize=8)
    else:
        s.get_legend().remove()

    s.tick_params(axis='both', labelsize=7)
    s.set_title(app, fontsize=8)
    s.set_xlabel('In Rate', fontsize=7)
    s.set_ylabel('Avg. Live JVM Threads', fontsize=7)

plt.subplots_adjust(
    top=0.8, 
    bottom=0.24, 
    left=0.12, 
    right=0.98, 
    hspace=0.3, 
    wspace=0.35
)
plt.savefig("plots/jvm_threads_results.pdf")


fig, ax = plt.subplots(1, 2, figsize=(7, 3))
for idx, app in enumerate(combined_df['app'].unique()):
    app_data = combined_df[combined_df['app'] == app]
    if idx ==0:
        hasLegend = False
    else:
        hasLegend = True
    s = sns.barplot(
        data=app_data,
        x='input',
        y='shuffle_netty_used_segments',
        hue='config',
        ax=ax[idx],
        ci=None,
        palette='husl',
        edgecolor='black',
        width=0.8, 
        hue_order=hue_order,
        # legend=hasLegend,
    )
    
    if hasLegend:
        s.legend(title='Config', fontsize=7, title_fontsize=8)
    else:
        s.get_legend().remove()

    s.tick_params(axis='both', labelsize=7)
    s.set_title(app, fontsize=8)
    s.set_xlabel('In Rate', fontsize=7)
    s.set_ylabel('Shuffle Netty Used Segments', fontsize=7)

plt.subplots_adjust(
    top=0.8, 
    bottom=0.24, 
    left=0.12, 
    right=0.98, 
    hspace=0.3, 
    wspace=0.35
)
plt.savefig("plots/shuffle_segments_results.pdf")


fig, ax = plt.subplots(1, 2, figsize=(7, 3))
for idx, app in enumerate(combined_df['app'].unique()):
    app_data = combined_df[combined_df['app'] == app]
    if idx ==0:
        hasLegend = False
    else:
        hasLegend = True
    s = sns.barplot(
        data=app_data,
        x='input',
        y='remote_bytes_per_sec',
        hue='config',
        ax=ax[idx],
        ci=None,
        palette='husl',
        edgecolor='black',
        width=0.8, 
        hue_order=hue_order,
        # legend=hasLegend,
    )
    
    if hasLegend:
        s.legend(title='Config', fontsize=7, title_fontsize=8)
    else:
        s.get_legend().remove()

    s.tick_params(axis='both', labelsize=7)
    s.set_title(app, fontsize=8)
    s.set_xlabel('In Rate', fontsize=7)
    s.set_ylabel('Bytes from Remote', fontsize=7)

plt.subplots_adjust(
    top=0.8, 
    bottom=0.24, 
    left=0.12, 
    right=0.98, 
    hspace=0.3, 
    wspace=0.35
)
plt.savefig("plots/remote_bytes_results.pdf")

# remove the combined CSV file
if os.path.exists("combined_results.csv"):
    os.remove("combined_results.csv")