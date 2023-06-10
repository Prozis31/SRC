import pandas as pd
import numpy as np
import pygeoip

def generate_typical_behavior(data):
    typical_behavior = pd.DataFrame(columns=['src_ip', 'up_bytes', 'down_bytes'])

    # Calculate the typical behavior based on the data
    grouped_data = data.groupby('src_ip').agg({'up_bytes': 'sum', 'down_bytes': 'sum'})
    typical_behavior['src_ip'] = grouped_data.index
    typical_behavior['up_bytes'] = grouped_data['up_bytes']
    typical_behavior['down_bytes'] = grouped_data['down_bytes']

    # Save the typical behavior to a CSV file
    typical_behavior.to_csv('typical_behavior.csv', index=False)

def generate_siem_rules(data, typical_behavior):
    # Calculate the threshold values for anomaly detection
    max_upload = typical_behavior['up_bytes'].max()
    max_download = typical_behavior['down_bytes'].max()

    upload_limit = max_upload * 1.1
    download_limit = max_download * 1.1

    # Create a DataFrame to store the compromised devices
    compromised_devices = pd.DataFrame(columns=['src_ip'])

    # Define the SIEM rules
    siem_rules = []

    # Rule 1: High upload traffic
    siem_rules.append("alert tcp any any -> any any (msg:\"High upload traffic\"; flow:to_server,established; bytes_out > {};)".format(upload_limit))

    # Rule 2: High download traffic
    siem_rules.append("alert tcp any any -> any any (msg:\"High download traffic\"; flow:to_client,established; bytes_in > {};)".format(download_limit))

    # Rule 3: Possible botnet activity
    typical_src_ips = typical_behavior['src_ip'].unique()
    siem_rules.append("alert tcp {} any -> any any (msg:\"Possible botnet activity\"; flow:to_server,established; bytes_out > {};)".format(','.join(typical_src_ips), max_upload))

    # Rule 4: Possible data exfiltration
    siem_rules.append("alert (tcp|udp) any any -> any any (msg:\"Possible data exfiltration\"; flow:established; bytes > {};)".format(max(max_upload, max_download)))

    # Rule 5: Possible C&C activity
    try:
        gi = pygeoip.GeoIP('GeoIP.dat')
        data['dst_country'] = data['dst_ip'].apply(lambda x: gi.country_code_by_addr(x))
        common_countries = data['dst_country'].value_counts().index[:5].tolist()
        siem_rules.append("alert (tcp|udp) any any -> any any (msg:\"Possible C&C activity\"; flow:established; !dst_country: {};)".format(','.join(common_countries)))
    except Exception as e:
        print("Error during IP geolocation:", str(e))

    # Detect compromised devices
    for src_ip in typical_src_ips:
        device_data = data[data['src_ip'] == src_ip]
        if device_data['up_bytes'].max() > upload_limit or device_data['down_bytes'].max() > download_limit:
            compromised_devices = compromised_devices.append({'src_ip': src_ip}, ignore_index=True)

    # Store the compromised devices in a text file
    compromised_devices.to_csv('compromised_devices.txt', index=False)

    return siem_rules

# Load the data from data8.parquet
data = pd.read_parquet('data8.parquet')

# Generate the typical behavior based on the data
generate_typical_behavior(data)

# Load the data and typical behavior DataFrame
data = pd.read_parquet('test8.parquet')
typical_behavior = pd.read_csv('typical_behavior.csv')

# Generate the SIEM rules and store the compromised devices
siem_rules = generate_siem_rules(data, typical_behavior)

# Print the SIEM rules
for rule in siem_rules:
    print(rule)
