import pandas as pd
import numpy as np
import ipaddress
import pygeoip
import dns.resolver
import dns.reversename

def load_data(file_path):
    # Load the parquet file into a pandas DataFrame
    data = pd.read_parquet(file_path)
    return data

def analyze_behavior(data):
    # Identify the typical behavior of network devices
    typical_behavior = data.groupby('src_ip').size().reset_index(name='count')
    typical_behavior = typical_behavior.sort_values(by='count', ascending=False)
    return typical_behavior

def perform_geolocation(ip_address):
    # Perform IP geolocation using GeoIP database
    geo_ip = pygeoip.GeoIP('GeoIP.dat')
    country = geo_ip.country_name_by_addr(ip_address)
    return country

def reverse_dns_lookup(ip_address):
    # Perform reverse DNS lookup
    reversed_ip = dns.reversename.from_address(ip_address)
    try:
        hostname = str(dns.resolver.resolve(reversed_ip, 'PTR')[0])
    except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer):
        hostname = 'Unknown'
    return hostname

def detect_anomalies(data, typical_behavior):
    # Extract the unique destination IP addresses and their corresponding countries from the data
    unique_dst_ips = data['dst_ip'].unique()
    unique_dst_countries = [perform_geolocation(ip) for ip in unique_dst_ips]

    # Identify the common countries based on destination IP addresses in the data
    common_countries = list(set(unique_dst_countries))

    max_upload = typical_behavior['up_bytes'].max()
    max_download = typical_behavior['down_bytes'].max()

    upload_limit = max_upload * 1.2
    download_limit = max_download * 1.2

    anomalies = []
    geo_ip_asn = pygeoip.GeoIP('GeoIPASNum.dat')
    columns_of_interest = ['timestamp', 'src_ip', 'dst_ip', 'anomaly', 'points_of_interest']

    for index, row in data.iterrows():
        timestamp = row['timestamp']
        src_ip = row['src_ip']
        dst_ip = row['dst_ip']
        up_bytes = row['up_bytes']
        down_bytes = row['down_bytes']

        if up_bytes > upload_limit:
            anomalies.append({'timestamp': timestamp, 'src_ip': src_ip, 'dst_ip': dst_ip, 'anomaly': 'High upload traffic', 'points_of_interest': []})

        if down_bytes > download_limit:
            anomalies.append({'timestamp': timestamp, 'src_ip': src_ip, 'dst_ip': dst_ip, 'anomaly': 'High download traffic', 'points_of_interest': []})

        if up_bytes > max_upload and src_ip in typical_behavior['src_ip'].values:
            anomalies.append({'timestamp': timestamp, 'src_ip': src_ip, 'dst_ip': dst_ip, 'anomaly': 'possible_botnet', 'points_of_interest': ['Possible botnet activity']})

        if up_bytes > upload_limit or down_bytes > download_limit:
            anomalies.append({'timestamp': timestamp, 'src_ip': src_ip, 'dst_ip': dst_ip, 'anomaly': 'possible_exfiltration', 'points_of_interest': ['Possible data exfiltration']})

        if perform_geolocation(dst_ip) != 'Unknown' and perform_geolocation(dst_ip) not in common_countries:
            anomalies.append({'timestamp': timestamp, 'src_ip': src_ip, 'dst_ip': dst_ip, 'anomaly': 'possible_c2', 'points_of_interest': ['Possible C&C activity']})

        if perform_geolocation(dst_ip) == 'Unknown':
            try:
                dns.resolver.resolve(dst_ip, 'A')
                anomalies.append({'timestamp': timestamp, 'src_ip': src_ip, 'dst_ip': dst_ip, 'anomaly': 'DNS resolution error', 'points_of_interest': []})
            except dns.exception.DNSException:
                anomalies.append({'timestamp': timestamp, 'src_ip': src_ip, 'dst_ip': dst_ip, 'anomaly': 'Non-existent domain', 'points_of_interest': []})

    for anomaly in anomalies:
        if 'Non-common country' not in anomaly['anomaly']:
            src_ip = anomaly['src_ip']
            dst_ip = anomaly['dst_ip']
            hostname = reverse_dns_lookup(src_ip)
            ports = data[(data['src_ip'] == src_ip) & (data['dst_ip'] == dst_ip)]['port'].unique()
            unique_ports_count = len(ports)
            points_of_interest = [timestamp, hostname, unique_ports_count] + anomaly['points_of_interest']
            anomaly['points_of_interest'] = points_of_interest

    return pd.DataFrame(anomalies, columns=columns_of_interest)


# Load the data from the full day file
data = load_data('data8.parquet')

# Analyze typical behavior of network devices
typical_behavior = analyze_behavior(data)

# Load the test data file for anomaly detection
test_data = load_data('test8.parquet')

# Detect anomalies in the test data
anomalies = detect_anomalies(test_data, typical_behavior)

# Save the detected anomalies to a CSV file
anomalies.to_csv('anomalies.csv', index=False)
