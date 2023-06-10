import pandas as pd

# Read the original CSV file
df = pd.read_csv('csvs/network_analysis_data.csv')

# Filter out rows with unwanted organizations
filtered_df = df[~df['organization'].str.contains('AS\d+ MICROSOFT-CORP-MSN-AS-BLOCK|AS\d+ GOOGLE', na=False)]

# Save the filtered DataFrame to a new CSV file
filtered_df.to_csv('csvs/filtered_data_network_analysis.csv', index=False)
