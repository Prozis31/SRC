import pandas as pd
import ipaddress
import pygeoip
import dns.resolver
import dns.reversename

datafile = 'test8.parquet'
geoip_file = 'GeoIP.dat'
geoipasnum_file = 'GeoIPASNum.dat'
geolocation_csv = 'geolocation_destinations.csv'
network_analysis_csv = 'network_analysis.csv'
destination_ports_csv = 'unique_destination_ports.csv'

# Read the CSV files
geolocation_destinations = pd.read_csv(geolocation_csv)
network_analysis = pd.read_csv(network_analysis_csv)
unique_destination_ports = pd.read_csv(destination_ports_csv)

# Read parquet data file
data = pd.read_parquet(datafile)

# Determine whether the destination IPv4 address is a public address
private_network = ipaddress.IPv4Network('192.168.100.0/24')
is_public_ip = data['dst_ip'].apply(lambda x: ipaddress.IPv4Address(x) not in private_network)

# Geolocation of public destination addresses
gi = pygeoip.GeoIP(geoipasnum_file)
gi_country = pygeoip.GeoIP(geoip_file)
cc = data[is_public_ip]['dst_ip'].apply(lambda x: gi_country.country_code_by_addr(x))
org = data[is_public_ip]['dst_ip'].apply(lambda x: gi.org_by_addr(x))

# Unique geolocation destinations
unique_destinations = pd.DataFrame({'country_code': cc, 'organization': org}).drop_duplicates()

# Compare geolocation destinations with the existing CSV file
new_destinations = unique_destinations.merge(geolocation_destinations, how='left', indicator=True)
new_destinations = new_destinations[new_destinations['_merge'] == 'left_only'].drop('_merge', axis=1)

# Calculate maximum upload, maximum download, and protocol with most data transfers for each source IP
max_upload = data.groupby('src_ip')['up_bytes'].max()
max_download = data.groupby('src_ip')['down_bytes'].max()
most_data_transfers = data.groupby('src_ip')['proto'].apply(lambda x: x.value_counts().index[0])

# Calculate average of each source IP
average_bytes = data.groupby('src_ip')[['up_bytes', 'down_bytes']].mean()

# Get unique destination ports
unique_ports = data['port'].unique()

# Compare unique destination ports with the existing CSV file
new_ports = pd.DataFrame({'destination_port': unique_ports}).merge(unique_destination_ports, how='left', indicator=True)
new_ports = new_ports[new_ports['_merge'] == 'left_only'].drop('_merge', axis=1)

# Perform DNS reverse lookup for source IP addresses
reverse_dns = data['src_ip'].apply(lambda x: str(dns.reversename.from_address(x)))

# Create a DataFrame with the results
results = pd.DataFrame({
    'src_ip': data['src_ip'].unique(),
    'max_upload': max_upload,
    'max_download': max_download,
    'proto_most_data_transfers': most_data_transfers,
    'average_upload': average_bytes['up_bytes'],
    'average_download': average_bytes['down_bytes'],
    'reverse_dns': reverse_dns
})

# Store the results in a new CSV file
results.to_csv('analysis_results.csv', index=False)
new_destinations.to_csv('new_geolocation_destinations.csv', index=False)
new_ports.to_csv('new_destination_ports.csv', index=False)
