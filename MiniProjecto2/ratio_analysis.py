import pandas as pd
import numpy as np

datafile = 'csvs/filtered_data_network_analysis.csv'
output_csv = 'csvs/ratio_analysis_noMG_data.csv'

# Read the network analysis data from CSV
data = pd.read_csv(datafile)

# Calculate the mean and standard deviation of ratio_down_up_bytes for each src_ip
mean_ratio = data.groupby('src_ip')['ratio_down_up_bytes'].mean()
std_ratio = data.groupby('src_ip')['ratio_down_up_bytes'].std()

# Calculate the overall mean and standard deviation of ratio_down_up_bytes
overall_mean_ratio = np.mean(data['ratio_down_up_bytes'])
overall_std_ratio = np.std(data['ratio_down_up_bytes'], ddof=0)

# Find the top 3 highest values of ratio_down_up_bytes for each src_ip
top_3_highest = data.groupby('src_ip').apply(lambda x: x.nlargest(3, 'ratio_down_up_bytes')).reset_index(drop=True)

# Create a DataFrame with the results
ratio_analysis_results = pd.DataFrame({
    'src_ip': mean_ratio.index,
    'mean_ratio': mean_ratio.values,
    'std_ratio': std_ratio.values
})

# Format mean_ratio and std_ratio columns to display up to 3 decimal points
ratio_analysis_results['mean_ratio'] = ratio_analysis_results['mean_ratio'].apply(lambda x: format(x, '.3f'))
ratio_analysis_results['std_ratio'] = ratio_analysis_results['std_ratio'].apply(lambda x: format(x, '.3f'))

# Create a row for the overall mean and standard deviation
overall_row = pd.DataFrame({
    'src_ip': ['Overall'],
    'mean_ratio': [format(overall_mean_ratio, '.3f')],
    'std_ratio': [format(overall_std_ratio, '.3f')]
})

# Concatenate the overall row with the ratio analysis results
ratio_analysis_results = pd.concat([ratio_analysis_results, overall_row], ignore_index=True)

# Store the ratio analysis results in a new CSV file
ratio_analysis_results.to_csv(output_csv, index=False)

# Store the top 3 highest values for each src_ip in a separate CSV file
top_3_highest.to_csv('csvs/top_3_highest_ratio_noMG_data.csv', index=False)
