import pandas as pd

# Read the first CSV file
file1 = 'csvs/network_analysis_test.csv'
data1 = pd.read_csv(file1)

# Read the second CSV file
file2 = 'csvs/unique_src_ips_botnets.csv'
data2 = pd.read_csv(file2)

# Extract the source IPs from both files
ips1 = set(data1['src_ip'])
ips2 = set(data2['src_ip'])

# Check if each IP in data1 is not present in data2
ips_not_in_data2 = [ip for ip in ips1 if ip not in ips2]

# Create a DataFrame with the results
result = pd.DataFrame({'src_ip': ips_not_in_data2})

# Save the results to a new CSV file
result.to_csv('csvs/ips_unique_src_ips_botnets.csv', index=False)
