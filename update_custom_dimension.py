#!/usr/bin/env python

from unificontrol import UnifiClient
import ssl
from kentikapi.v5 import tagging
import csv
import time
from decouple import config

UNIFI_USER = config('UNIFI_USER', default='')
UNIFI_PASSWORD = config('UNIFI_PASSWORD', default='')
UNIFI_SITE = 'default'
UNIFI_HOST = 'net-tools.ryburn.org'
cert = ssl.get_server_certificate((UNIFI_HOST, 8443))
csvfile = 'clients.csv'

# ---------------------------------------------------
# Kentik options:
option_api_email = config('KENTIK_API_EMAIL', default='')
option_api_token = config('KENTIK_API_TOKEN', default='')
src_custom_dimension = 'c_src_dyn_hostname'
dst_custom_dimension = 'c_dst_dyn_hostname'
# ---------------------------------------------------


def pull_clients():
    print("Grabbing client list from controller...")
    client = UnifiClient(host=UNIFI_HOST, port=8443, username=UNIFI_USER, password=UNIFI_PASSWORD, site=UNIFI_SITE)
    client_list = client.list_clients()

    print("Writing client list to a file...")
    outfile = open(csvfile, "w")
    # write a header row for the CSV file
    outfile.write("mac_addr,hostname\n")
    for client in client_list:
        mac_addr = client['mac']
        if 'name' in client:
            hostname = client['name']
        elif 'hostname' in client:
            hostname = client['hostname']
        else:
            hostname = client['oui']
        outfile.write(mac_addr + ',' + hostname + '\n')
    outfile.close()


def push_to_kentik(direction):
    # -----
    # initialize a batch that will replace all populators
    # -----
    batch = tagging.Batch(True)
    crit = tagging.Criteria(direction)  # 'src', 'dst', or 'either'
    # Determine direction so we can set the CD name
    if direction == 'src':
        print("Building batch for src direction...")
        option_custom_dimension = src_custom_dimension
    elif direction == 'dst':
        print("Building batch for dst direction...")
        option_custom_dimension = dst_custom_dimension

    infile = open(csvfile, "r")
    csvreader = csv.reader(infile)

    # Read in the headers
    header = csvreader.__next__()
    macindex = header.index("mac_addr")
    nameindex = header.index("hostname")

    # Go line by line through the file and create criteria for all the data
    for row in csvreader:
        tag_name = row[nameindex]
        mac = row[macindex]
        print("Adding tag_name %s with a mac address value of %s" % (tag_name, mac))
        crit.add_mac_address(mac)

        batch.add_upsert(tag_name, crit)

    # -----
    # Showtime! Submit the batch as populators for the configured custom dimension
    # - library will take care of chunking the requests into smaller HTTP payloads
    # -----
    print("Submitting %s batch now..." % direction)
    client = tagging.Client(option_api_email, option_api_token)
    guid = client.submit_populator_batch(option_custom_dimension, batch)

    for x in range(1, 100):
        time.sleep(5)
        status = client.fetch_batch_status(guid)
        if status.is_finished():
            print(status.pretty_response())
            break


if __name__ == "__main__":
    pull_clients()
    push_to_kentik('src')
    push_to_kentik('dst')
