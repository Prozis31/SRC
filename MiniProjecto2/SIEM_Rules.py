import pandas as pd
import re
import pygeoip

def update_flows(flows, geoip_file, geoipasnum_file):
    # Geolocation of public destination addresses
    gi_country = pygeoip.GeoIP(geoip_file)
    gi_asnum = pygeoip.GeoIP(geoipasnum_file)

    # Calculate country code and organization
    flows['country_code'] = flows['dst_ip'].apply(lambda x: gi_country.country_code_by_addr(x) if pd.notnull(x) else None)
    flows['organization'] = flows['dst_ip'].apply(lambda x: gi_asnum.org_by_addr(x) if pd.notnull(x) else None)

    # Convert timestamp to datetime format
    flows['timestamp'] = pd.to_datetime(flows['timestamp'] * 10**7)  # Convert to nanoseconds (1/100th of a second precision)

    # Sort flows by timestamp
    flows = flows.sort_values('timestamp')

    # Calculate the number of connections in a minute for each unique combination
    flows['connections_per_1min'] = flows.groupby(['src_ip', 'dst_ip', pd.Grouper(key='timestamp', freq='1Min')])['src_ip'].transform('count')

    # Calculate the ratio of down_bytes to up_bytes for each unique combination
    flows['ratio_down_up_bytes'] = flows.groupby(['src_ip', 'dst_ip']).apply(lambda x: x['down_bytes'].sum() / x['up_bytes'].sum()).reset_index(drop=True)

    return flows


def filter_src_dst_dst_ip(flows):
    dst_ip_pattern = '192.168.108.[0-9]{3}'

    filtered_flows = flows[flows['dst_ip'].str.match(dst_ip_pattern) & (flows['dst_ip'] != '192.168.108.240')]

    return filtered_flows


def filter_unwanted_organizations(flows):
    unwanted_organizations = ['AS\d+ MICROSOFT-CORP-MSN-AS-BLOCK', 'AS\d+ GOOGLE']
    
    # Filter flows with (down_bytes / up_bytes) < 90
    flows1 = flows[flows['down_bytes'] / flows['up_bytes'] >= 90]
    
    # Filter flows with low number of connections per 1 min (< 30)
    flows2 = flows[flows['connections_per_1min'] >= 30]
    
    # Merge the two filtered flows
    filtered_flows = pd.concat([flows1, flows2])
    
    # Filter out rows with unwanted organizations
    filtered_flows = filtered_flows[~filtered_flows['organization'].str.contains('|'.join(unwanted_organizations), na=False)]
    
    return filtered_flows


def filter_values_not_present(flows, protocols, country_codes, organizations, destination_ports):
    filtered_flows = flows[
        ~flows['proto'].isin(protocols) |
        ~flows['country_code'].isin(country_codes) |
        ~flows['organization'].isin(organizations) |
        ~flows['port'].isin(destination_ports)
    ]
    
    return filtered_flows


# Step 0: Load data from CSV files
protocols = pd.read_csv('csvs/unique_protocols_data.csv')['protocol'].tolist()
country_codes = pd.read_csv('csvs/unique_countries_data.csv')['country_code'].tolist()
organizations = pd.read_csv('csvs/unique_organizations_data.csv')['organization'].tolist()
destination_ports = pd.read_csv('csvs/unique_destination_ports_data.csv')['destination_port'].tolist()

# Step 1: Read test8.parquet file
flows = pd.read_parquet('test8.parquet')

# Step 2: Update flows with additional information
geoip_file = 'GeoIP.dat'
geoipasnum_file = 'GeoIPASNum.dat'
flows = update_flows(flows, geoip_file, geoipasnum_file)


# Step 3: Filter flows1 based on src_dst and dst_ip patterns
flows1 = filter_src_dst_dst_ip(flows)

# Step 4: Filter flows2 out rows with unwanted organizations
flows2 = filter_unwanted_organizations(flows)

# Step 5: Filter flows3 with values not present in the provided CSV files
flows3 = filter_values_not_present(flows, protocols, country_codes, organizations, destination_ports)

# Merge the filtered flows together
filtered_flows = pd.concat([flows1, flows2, flows3]).drop_duplicates()

# Save src_ips to a CSV file
src_ips = filtered_flows['src_ip'].unique()
df_src_ips = pd.DataFrame({'src_ip': src_ips})
df_src_ips.to_csv('csvs/compromised_src_ips.csv', index=False)