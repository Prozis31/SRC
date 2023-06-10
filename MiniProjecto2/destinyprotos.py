import pandas as pd

datafile = 'test8.parquet'
output_csv = 'csvs/unique_protocols_test.csv'

# Read parquet data files
data = pd.read_parquet(datafile)

# Get unique protocols
unique_protocols = data['proto'].unique()

# Create a DataFrame with the unique protocols
unique_protocols_df = pd.DataFrame({'protocol': unique_protocols})

# Store the unique protocols in a CSV file
unique_protocols_df.to_csv(output_csv, index=False)
