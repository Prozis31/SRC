import pandas as pd

# Read the CSV file containing the source IPs not in data2
ips_file = 'csvs/ips_not_in_data2.csv'
ips_data = pd.read_csv(ips_file)

# Extract the source IPs from the file
ips = ips_data['src_ip']

# Read the original data file
datafile = 'csvs/network_analysis_test.csv'
data = pd.read_csv(datafile)

# Filter connections based on the source IPs
filtered_data = data[data['src_ip'].isin(ips)]

# Save the filtered connections to a new CSV file
filtered_data.to_csv('csvs/filtered_connections.csv', index=False)
#---

# Extract the IPs from the file
ips = ips_data['src_ip']

# Filter the connections based on the source IPs in ips_data
filtered_data = filtered_data[filtered_data['src_ip'].isin(ips)]

# Define a function to get the top 3 highest values
def get_top_3_highest(values):
    return values.nlargest(3).tolist()

# Group by unique src_ip and calculate the median and top 3 highest values of percentage_down_up_bytes
grouped_data = filtered_data.groupby('src_ip')['ratio_down_up_bytes'].agg(['median', get_top_3_highest]).reset_index()

# Rename the column for top 3 highest values
grouped_data.rename(columns={'get_top_3_highest': 'top_3_highest'}, inplace=True)

# Save the results to a new CSV file
grouped_data.to_csv('csvs/median_top_3_highest.csv', index=False)