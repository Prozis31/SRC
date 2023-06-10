import pandas as pd

# Read the geolocation_destinations_data.csv file
geolocation_data = pd.read_csv('csvs/geolocation_destinations_test.csv')

# Get unique organizations
unique_organizations = geolocation_data['organization'].unique()

# Create a DataFrame with unique organizations
unique_organizations_df = pd.DataFrame({'organization': unique_organizations})

# Store the unique organizations in a new CSV file
unique_organizations_df.to_csv('csvs/unique_organizations_test.csv', index=False)

# Get unique countries
unique_countries = geolocation_data['country_code'].unique()

# Create a DataFrame with unique countries
unique_countries_df = pd.DataFrame({'country_code': unique_countries})

# Store the unique countries in a new CSV file
unique_countries_df.to_csv('csvs/unique_countries_test.csv', index=False)
