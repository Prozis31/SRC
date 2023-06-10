import pandas as pd

# Read the network_analysis_test.csv file
network_analysis = pd.read_csv('csvs/network_analysis_test.csv')

# Read the country_code_not_in_data2.csv file
country_codes = pd.read_csv('csvs/country_code_not_in_data2.csv')

# Read the organization_not_in_data2.csv file
organizations = pd.read_csv('csvs/organization_not_in_data2.csv')

# Filter connections based on country code or organization
filtered_connections = network_analysis[
    network_analysis['country_code'].isin(country_codes['country_code']) |
    network_analysis['organization'].isin(organizations['organization'])
]


# Save the filtered connections to a new CSV file
filtered_connections.to_csv('csvs/filtered_connections_countries_organizations.csv', index=False)
