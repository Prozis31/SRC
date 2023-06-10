import pandas as pd
from scipy import stats
import ipaddress
import pygeoip
import numpy
# Load geolocation databases
gi = pygeoip.GeoIP('./GeoIP.dat')
gi2 = pygeoip.GeoIP('./GeoIPASNum.dat')

# Load data
df_train = pd.read_parquet('data8.parquet')
df_test = pd.read_parquet('test8.parquet')

# Define private network
NET = ipaddress.IPv4Network('192.168.100.0/24')

# Add a column indicating if destination IP is public or private
df_train['is_public'] = df_train['dst_ip'].apply(lambda x: ipaddress.IPv4Address(x) not in NET)
df_test['is_public'] = df_test['dst_ip'].apply(lambda x: ipaddress.IPv4Address(x) not in NET)

# Define typical behavior
typical_behavior = df_train.describe()

# Add geolocation for public IPs
df_train['cc'] = df_train[df_train['is_public']]['dst_ip'].apply(lambda y: gi.country_code_by_addr(y))
df_test['cc'] = df_test[df_test['is_public']]['dst_ip'].apply(lambda y: gi.country_code_by_addr(y))

# Detect anomalous behavior in test data using z-score
df_test['z_score_up'] = stats.zscore(df_test['up_bytes'])
df_test['z_score_down'] = stats.zscore(df_test['down_bytes'])

# Define thresholds for anomalous behavior
threshold_zscore = 3
threshold_connection_count = 1000
threshold_upload_sum = 1e9
threshold_unique_ports = 10
standard_ports = [20, 21, 22, 23, 25, 53, 80, 110, 115, 135, 139, 143, 161, 162, 443, 445, 3389]

# Define anomalous behavior
df_test['anomaly_up'] = df_test['z_score_up'].apply(lambda x: x > threshold_zscore)
df_test['anomaly_down'] = df_test['z_score_down'].apply(lambda x: x > threshold_zscore)
df_test['connection_count'] = df_test.groupby('src_ip')['src_ip'].transform('count')
df_test['possible_botnet'] = df_test['connection_count'] > threshold_connection_count
df_test['upload_sum'] = df_test.groupby('src_ip')['up_bytes'].transform('sum')
df_test['possible_exfiltration'] = df_test['upload_sum'] > threshold_upload_sum

# Detect usage of non-standard ports
df_test['non_standard_port'] = ~df_test['port'].isin(standard_ports)
df_test['unique_ports_count'] = df_test[df_test['non_standard_port']].groupby('src_ip')['port'].transform('nunique')
df_test['possible_c2'] = df_test['unique_ports_count'] > threshold_unique_ports

# Define SIEM rules based on detected anomalies
siem_rules = {
    'rule1': {'name': 'High upload traffic', 'condition': df_test['anomaly_up']},
    'rule2': {'name': 'High download traffic', 'condition': df_test['anomaly_down']},
    'rule3': {'name': 'Possible botnet activity', 'condition': df_test['possible_botnet']},
    'rule4': {'name': 'Possible data exfiltration', 'condition': df_test['possible_exfiltration']},
    'rule5': {'name': 'Possible C&C activity', 'condition': df_test['possible_c2']}
}

# Test SIEM rules and identify devices with anomalous behavior
df_test['High upload traffic'] = df_test['anomaly_up']
df_test['High download traffic'] = df_test['anomaly_down']
df_test['Possible botnet activity'] = df_test['possible_botnet']
df_test['Possible data exfiltration'] = df_test['possible_exfiltration']
df_test['Possible C&C activity'] = df_test['possible_c2']

# Generate a report of detected anomalies
report = df_test[df_test['High upload traffic'] | df_test['High download traffic'] | df_test['Possible botnet activity'] | df_test['Possible data exfiltration'] | df_test['Possible C&C activity']]

# Add new columns for analysis results
df_test['Activity'] = 'Normal'
df_test['Cause'] = 'Not applicable'
df_test['Dst IP distribution'] = np.nan
df_test['Protocol distribution'] = np.nan
df_test['Port distribution'] = np.nan
df_test['Total upload bytes'] = np.nan
df_test['Total download bytes'] = np.nan

# Iterate over unique source IPs
for ip in df_test['src_ip'].unique():
    # Get data for this source IP
    data_ip = df_test[df_test['src_ip'] == ip]
    
    # Calculate the distribution of destination IPs, protocols, and ports
    dst_ip_dist = data_ip['dst_ip'].value_counts(normalize=True)
    proto_dist = data_ip['proto'].value_counts(normalize=True)
    port_dist = data_ip['port'].value_counts(normalize=True)
    
    # Calculate total upload/download bytes
    total_up_bytes = data_ip['up_bytes'].sum()
    total_down_bytes = data_ip['down_bytes'].sum()
    
    # Update the analysis results
    df_test.loc[df_test['src_ip'] == ip, 'Dst IP distribution'] = str(dst_ip_dist)
    df_test.loc[df_test['src_ip'] == ip, 'Protocol distribution'] = str(proto_dist)
    df_test.loc[df_test['src_ip'] == ip, 'Port distribution'] = str(port_dist)
    df_test.loc[df_test['src_ip'] == ip, 'Total upload bytes'] = total_up_bytes
    df_test.loc[df_test['src_ip'] == ip, 'Total download bytes'] = total_down_bytes

# Write a summary of the analysis to a text file
with open('detailed_analysis.txt', 'w') as f:
    for ip in df_test['src_ip'].unique():
        data_ip = df_test[df_test['src_ip'] == ip]
        f.write(f"IP: {ip}\n")
        f.write(f"Activity: {data_ip['Activity'].iloc[0]}\n")
        f.write(f"Cause: {data_ip['Cause'].iloc[0]}\n")
        f.write("Destination IP distribution:\n")
        f.write(data_ip['Dst IP distribution'].iloc[0] + "\n")
        f.write("Protocol distribution:\n")
        f.write(data_ip['Protocol distribution'].iloc[0] + "\n")
        f.write("Port distribution:\n")
        f.write(data_ip['Port distribution'].iloc[0] + "\n")
        f.write(f"Total upload bytes: {data_ip['Total upload bytes'].iloc[0]}\n")
        f.write(f"Total download bytes: {data_ip['Total download bytes'].iloc[0]}\n")
        f.write("\n")


# Save report as csv
report.to_csv('anomaly_report.csv')
