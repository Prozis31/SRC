import pandas as pd

# Read the analysis_results.csv file
analysis_results = pd.read_csv('csvs/network_analysis_data.csv')

# Sort the DataFrame by percentage_down_up_bytes column in descending order
sorted_results = analysis_results.sort_values('percentage_down_up_bytes', ascending=False)

# Select the highest 50 rows
highest_50 = sorted_results.head(50)

# Select the lowest 50 rows
lowest_50 = sorted_results.tail(50)

# Concatenate the highest and lowest rows
selected_results = pd.concat([highest_50, lowest_50])

# Save the selected results to a new CSV file
selected_results.to_csv('csvs/filtered_analysis_results_test.csv', index=False)

