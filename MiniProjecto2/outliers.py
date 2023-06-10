import pandas as pd

# Read the data from the original CSV file
data = pd.read_csv('csvs/network_analysis_test.csv')

# Filter the data for port 443
filtered_data = data[data['port'] == 443]

# Calculate the mean of num_connections
mean_num_connections = filtered_data['num_connections'].mean()

# Determine outliers using 1.5 times the IQR away from the median
Q1 = filtered_data['num_connections'].quantile(0.25)
Q3 = filtered_data['num_connections'].quantile(0.75)
IQR = Q3 - Q1
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR
outliers = filtered_data[(filtered_data['num_connections'] < lower_bound) | (filtered_data['num_connections'] > upper_bound)]

# Filter outliers with dst_ip matching the pattern "192.168.108.*"
pattern = '192.168.108.[0-9]{3}'
filtered_outliers = outliers[outliers['dst_ip'].str.match(pattern)]

# Remove entries with dst_ip equal to "192.168.108.240"
filtered_outliers = filtered_outliers[filtered_outliers['dst_ip'] != '192.168.108.240']

# Select the desired fields for the new CSV
formatted_data = filtered_outliers[['src_ip', 'dst_ip', 'port', 'ratio_down_up_bytes', 'num_connections']]

# Save the formatted data to a new CSV file
formatted_data.to_csv('csvs/outliers_formatted_norouter.csv', index=False)
