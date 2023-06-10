import pandas as pd
import numpy as np
import ipaddress
import pygeoip

datafile = 'test8.parquet'
output_csv = 'csvs/network_analysis_test.csv'
threshold_connections = 100
connection_window = '5T'  # 5 minutes
geoip_file = 'GeoIP.dat'
geoipasnum_file = 'GeoIPASNum.dat'

# Read parquet data file
data = pd.read_parquet(datafile)

# Convert timestamp column to datetime format
data['timestamp'] = pd.to_datetime(data['timestamp'], unit='s')

# Group by unique source IP, destination IP, and port
grouped_data = data.groupby(['src_ip', 'dst_ip', 'port'])

# Calculate the total time of the connections for each unique combination
total_time = grouped_data.apply(lambda x: (x['timestamp'].max() - x['timestamp'].min()).total_seconds())

# Calculate the ratio of down_bytes to up_bytes for each unique combination
ratio_down_up_bytes = grouped_data.apply(lambda x: x['down_bytes'].sum() / x['up_bytes'].sum())

# Calculate the number of attempted connections in each unique combination over a 5-minute window
num_connections = grouped_data.size()

# Calculate the total number of up_bytes and down_bytes in each unique combination
total_up_bytes = grouped_data['up_bytes'].sum()
total_down_bytes = grouped_data['down_bytes'].sum()

# Calculate the maximum number of connections that occur in a 5-minute window for each unique combination
max_connections_5min = grouped_data.apply(lambda x: x.set_index('timestamp').resample(connection_window).size().max())

# Create a DataFrame with the results
analysis_results = pd.DataFrame({
    'src_ip': grouped_data['src_ip'].first(),
    'dst_ip': grouped_data['dst_ip'].first(),
    'port': grouped_data['port'].first(),
    'total_time_sec': total_time.reindex(grouped_data.groups.keys()),
    'ratio_down_up_bytes': ratio_down_up_bytes.reindex(grouped_data.groups.keys()),
    'num_connections': num_connections,
    'total_up_bytes': total_up_bytes.reindex(grouped_data.groups.keys()),
    'total_down_bytes': total_down_bytes.reindex(grouped_data.groups.keys()),
    'max_connections_5min': max_connections_5min.reindex(grouped_data.groups.keys())
})

# Add a flag column for flows with too high connections over a 5-minute window
analysis_results['flag_high_connections'] = analysis_results['num_connections'] > threshold_connections

# Determining whether the destination IPv4 address is a public address
private_network = ipaddress.IPv4Network('192.168.100.0/24')
is_public_ip = analysis_results['dst_ip'].apply(lambda x: ipaddress.IPv4Address(x) not in private_network)

# Geolocation of public destination addresses
gi_country = pygeoip.GeoIP(geoip_file)
gi_asnum = pygeoip.GeoIP(geoipasnum_file)

analysis_results['country_code'] = analysis_results[is_public_ip]['dst_ip'].apply(lambda x: gi_country.country_code_by_addr(x))
analysis_results['organization'] = analysis_results[is_public_ip]['dst_ip'].apply(lambda x: gi_asnum.org_by_addr(x))

# Format the ratio_down_up_bytes column to display up to 3 decimal points
analysis_results['ratio_down_up_bytes'] = analysis_results['ratio_down_up_bytes'].apply(lambda x: format(x, '.3f'))

# Store the analysis results in a CSV file
analysis_results.to_csv(output_csv, index=False)
