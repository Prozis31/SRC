import pandas as pd

# Load the Parquet file with legitimate traffic
baseline_df = pd.read_parquet("data8.parquet")

# Calculate the baseline for uploaded and downloaded bytes
baseline_flows = baseline_df.groupby(['src_ip', 'dst_ip']).agg({'up_bytes': 'sum', 'down_bytes': 'sum'}).reset_index()
baseline_up_bytes = baseline_flows['up_bytes'].mean()
baseline_down_bytes = baseline_flows['down_bytes'].mean()

# Adjust the botnet_threshold based on the calculated baseline and risk tolerance
botnet_threshold = 0.25 * min(baseline_up_bytes, baseline_down_bytes)

# Load the Parquet file with potential botnet or C&C activity
df = pd.read_parquet("test8.parquet")

# Filter for relevant columns
df = df[['timestamp', 'src_ip', 'dst_ip', 'proto', 'port', 'up_bytes', 'down_bytes']]

# Group flows by source IP and destination IP
grouped = df.groupby(['src_ip', 'dst_ip'])

# Calculate the total bytes transferred for each flow
flows = grouped.agg({'up_bytes': 'sum', 'down_bytes': 'sum'}).reset_index()

# Detect suspicious flows based on the adjusted threshold
suspicious_flows = flows[(flows['up_bytes'] > botnet_threshold) | (flows['down_bytes'] > botnet_threshold)]

# Get unique source IPs
unique_src_ips = flows['src_ip'].unique()

# Store unique source IPs in a CSV file
pd.DataFrame(unique_src_ips, columns=['src_ip']).to_csv('csvs/unique_src_ips_botnets.csv', index=False)
