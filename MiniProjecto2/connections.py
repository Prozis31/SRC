import pandas as pd
import numpy as np
import ipaddress
import pygeoip

datafile = 'data8.parquet'
output_csv = 'csvs/geolocation_destinations_data.csv'

# Read parquet data files
data = pd.read_parquet(datafile)

# Determining whether the destination IPv4 address is a public address
private_network = ipaddress.IPv4Network('192.168.100.0/24')
is_public_ip = data['dst_ip'].apply(lambda x: ipaddress.IPv4Address(x) not in private_network)

# Geolocation of public destination addresses
gi = pygeoip.GeoIP('./GeoIPASNum.dat')
gi_country = pygeoip.GeoIP('./GeoIP.dat')
cc = data.loc[is_public_ip, 'dst_ip'].apply(lambda x: gi_country.country_code_by_addr(x))
org = data.loc[is_public_ip, 'dst_ip'].apply(lambda x: gi.org_by_addr(x))

# Unique geolocation destinations
unique_destinations = pd.DataFrame({'country_code': cc, 'organization': org}).drop_duplicates()

# Store unique geolocation destinations in a CSV file
unique_destinations.to_csv(output_csv, index=False)
