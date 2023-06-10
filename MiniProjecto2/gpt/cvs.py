import pandas as pd

# Read the CSV file into a pandas DataFrame
data = pd.read_csv('anomaly_report.csv')

# Select the columns of interest
columns_of_interest = ['possible_botnet', 'possible_exfiltration', 'unique_ports_count',
                       'possible_c2', 'High upload traffic', 'High download traffic',
                       'Possible botnet activity', 'Possible data exfiltration',
                       'Possible C&C activity']
selected_data = data[columns_of_interest]

# Analyze the data
total_records = selected_data.shape[0]
botnet_count = selected_data['possible_botnet'].sum()
exfiltration_count = selected_data['possible_exfiltration'].sum()
unique_ports_count = selected_data['unique_ports_count'].sum()
c2_count = selected_data['possible_c2'].sum()
upload_traffic_count = selected_data['High upload traffic'].sum()
download_traffic_count = selected_data['High download traffic'].sum()
botnet_activity_count = selected_data['Possible botnet activity'].sum()
data_exfiltration_count = selected_data['Possible data exfiltration'].sum()
cc_activity_count = selected_data['Possible C&C activity'].sum()

# Print the analysis results
print("Total Records:", total_records)
print("Possible Botnet Count:", botnet_count)
print("Possible Exfiltration Count:", exfiltration_count)
print("Unique Ports Count:", unique_ports_count)
print("Possible C2 Count:", c2_count)
print("High Upload Traffic Count:", upload_traffic_count)
print("High Download Traffic Count:", download_traffic_count)
print("Possible Botnet Activity Count:", botnet_activity_count)
print("Possible Data Exfiltration Count:", data_exfiltration_count)
print("Possible C&C Activity Count:", cc_activity_count)
