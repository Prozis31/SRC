import pandas as pd

datafile = 'test8.parquet'
output_csv = 'csvs/unique_destination_ports_test.csv'

# Read parquet data files
data = pd.read_parquet(datafile)

# Get unique destination ports
unique_ports = data['port'].unique()

# Create a DataFrame with the unique destination ports
unique_ports_df = pd.DataFrame({'destination_port': unique_ports})

# Store the unique destination ports in a CSV file
unique_ports_df.to_csv(output_csv, index=False)
