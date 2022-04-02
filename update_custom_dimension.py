#!/usr/bin/env python
#
#   update_custom_dimension.py  written by Justin Ryburn (jryburn@kentik.com) 2022 Apr 1
#
#   This simple python script grabs a client list from a Ubiquiti Unifi controller and saves them to a .csv file.
#  It then uses the Kentik bulk API to quickly load custom dimension populator data.
#
#   This script uses the Kentik bulk API written by Blake Caldwell which can be located at:
#   https://github.com/kentik/kentikapi-py/tree/master/kentikapi/v5

from unificontrol import UnifiClient
from kentikapi.v5 import tagging
from decouple import config
from collections import defaultdict
import csv
import time
import ssl

################################
# This section are the user editable variables the script relies upon
# secrets are stored in the .env file in the same directory as the script.
# The config function pulls these in to the script.
UNIFI_USER = config('UNIFI_USER')
UNIFI_PASSWORD = config('UNIFI_PASSWORD')
UNIFI_SITE = 'default'
UNIFI_HOST = 'net-tools.ryburn.org'
cert = ssl.get_server_certificate((UNIFI_HOST, 8443))
# Name of the CSV file we are going to store the client list in
csvfile = 'clients.csv'

################################
# Kentik options:
option_api_email = config('KENTIK_API_EMAIL')
option_api_token = config('KENTIK_API_TOKEN')
#
# "populator_name" and "populator_data" should be the column headers for the CSV file.
#
populator_name = "hostname"
populator_data = "mac_addr"
# This script automatically creates both the source and destination populators for a given custom dimension.
# In the Kentik portal, you need to create two custom dimensions. The database fields must be in the format of
# "c_dst_dimension" and "c_src_dimension". The script will add the populators and MAC address to both the source
# and destination dimensions.
src_custom_dimension = 'c_src_dyn_hostname'
dst_custom_dimension = 'c_dst_dyn_hostname'
#
#  End of user variables
################################

tags = defaultdict(list)


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


def push_to_kentik():
    # initialize a batch that will replace all populators
    batch_src = tagging.Batch(True)
    batch_dst = tagging.Batch(True)

    # Use the CSV reader library to read in the CSV file and send the data to the batch API
    # The column headings should be labeled mac_addr and hostname

    print("Reading CSV file...")
    with open(csvfile, mode='r', encoding='utf-8-sig') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            populator = row[populator_name]
            mac = row[populator_data]
            tags[populator].append(mac)
    # print(tags)
    # Now iterate through the tags and add them to the batch. We're creating
    # two batches (for src and dst) so we need two criteria, addresses and populators)
    print("Building batch for upload...")
    for item in tags.items():
        populator = item[0]
        mac = item[1][0]
        print("Adding populator %s with a mac address value of %s" % (populator, mac))
        crit_dst = tagging.Criteria("dst")
        crit_src = tagging.Criteria("src")
        crit_dst.add_mac_address(mac)
        crit_src.add_mac_address(mac)
        batch_dst.add_upsert(populator, crit_dst)
        batch_src.add_upsert(populator, crit_src)

    # -----
    # Showtime! Submit the batch as populators for the configured custom dimension
    # - library will take care of chunking the requests into smaller HTTP payloads
    # -----
    print("Submitting batch now...")
    client = tagging.Client(option_api_email, option_api_token)
    guid_dst = client.submit_populator_batch(dst_custom_dimension, batch_dst)
    guid_src = client.submit_populator_batch(src_custom_dimension, batch_src)

    #  Wait and display the results
    for x in range(1, 12):
        time.sleep(5)
        status = client.fetch_batch_status(guid_dst)
        if status.is_finished():
            print(status.pretty_response())
            break

    #  Wait and display the results
    for x in range(1, 12):
        time.sleep(5)
        status = client.fetch_batch_status(guid_src)
        if status.is_finished():
            print(status.pretty_response())
            break


if __name__ == "__main__":
    pull_clients()
    push_to_kentik()
