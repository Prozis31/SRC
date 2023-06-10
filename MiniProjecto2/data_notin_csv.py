import pandas as pd

# Read the first CSV file
file1 = 'csvs/unique_organizations_test.csv'
data1 = pd.read_csv(file1)

# Read the second CSV file
file2 = 'csvs/unique_organizations_data.csv'
data2 = pd.read_csv(file2)

# Extract the source IPs from both files
ips1 = set(data1['organization'])
ips2 = set(data2['organization'])

# Check if each IP in data1 is not present in data2
ips_not_in_data2 = [ip for ip in ips1 if ip not in ips2]

# Create a DataFrame with the results
result = pd.DataFrame({'organization': ips_not_in_data2})

# Save the results to a new CSV file
result.to_csv('csvs/organization_not_in_data2.csv', index=False)
