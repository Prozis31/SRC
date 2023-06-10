import pandas as pd

# Read the filtered_connections.csv file
filtered_connections = pd.read_csv('csvs/filtered_connections_countries_organizations.csv')

# Get unique src_ip values
unique_src_ips = filtered_connections['src_ip'].unique()

# Create a DataFrame with unique src_ips
df_unique_src_ips = pd.DataFrame({'src_ip': unique_src_ips})

# Save the unique src_ips to a new CSV file
df_unique_src_ips.to_csv('csvs/unique_src_ips_malicious.csv', index=False)
