import csv
import json
import time
from datetime import datetime

import pandas as pd
import pytz
import requests
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2


def read_pb(url):
    """
    Parse's .pb file
    :param url: url to .pb file
    :return: parsed content as dict
    """
    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(url)
    feed.ParseFromString(response.content)
    dict_obj = MessageToDict(feed)
    return dict_obj


def write_to_json(object, filename):
    """
    Write dict into a json formatted .txt file (large)
    :param dict_obj:
    :return:
    """
    file_path = f"{filename}.txt"
    with open(file_path, 'w') as file:
        json.dump(object, file, indent=4)


def convert_posix_to_mst(posix_timestamp):
    # Convert POSIX timestamp to a datetime object in UTC
    utc_datetime = datetime.utcfromtimestamp(
        posix_timestamp).replace(tzinfo=pytz.utc)

    # Convert UTC datetime to MST
    mst_timezone = pytz.timezone("America/Denver")
    mst_datetime = utc_datetime.astimezone(mst_timezone)

    return mst_datetime


def create_dict(id, routeid, stopid, delay, lastupdate):
    # Create a dictionary with the specified variables
    data_dict = {
        'id': id,
        'routeid': routeid,
        'stopid': stopid,
        'delay': delay,
        'lastupdate': lastupdate
    }
    return data_dict


def write_csv_from_dict_list(data, filename):

    file_path = f"{filename}.txt"

    if not data:
        print("Data is empty")
        return

    fieldnames = list(data[0].keys())

    with open(file_path, 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        # Write the header
        writer.writeheader()

        # Write the data
        writer.writerows(data)

    print(f'CSV file "{file_path}" created successfully.')


def append_dict_list_to_csv(data, csv_file_path):

    if not data:
        print("Data is empty, nothing to append")
        return

    fieldnames = list(data[0].keys())

    with open(csv_file_path, 'a', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        # If the file is empty, write the header
        if csv_file.tell() == 0:
            writer.writeheader()

        # Write the data
        writer.writerows(data)

    print(f'Data appended to CSV file "{csv_file_path}" successfully.')


def remove_duplicates_from_csv(csv_file_path):
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(csv_file_path)

        # Get the number of rows before removing duplicates
        initial_row_count = len(df)

        # Remove duplicates based on all columns
        df.drop_duplicates(inplace=True)

        # Write the DataFrame back to the CSV file
        df.to_csv(csv_file_path, index=False)

        # Calculate the number of duplicates removed
        duplicate_count = initial_row_count - len(df)

        print(
            f'{duplicate_count} duplicates removed from CSV file "{csv_file_path}" successfully.')
        return duplicate_count

    except FileNotFoundError:
        print(f"File '{csv_file_path}' not found.")
        return 0  # No duplicates removed since file not found


def main():
    """
    Vehicle Positions: http://gtfs.edmonton.ca/TMGTFSRealTimeWebService/Vehicle/VehiclePositions.pb
    Trip Updates: http://gtfs.edmonton.ca/TMGTFSRealTimeWebService/TripUpdate/TripUpdates.pb
    Alerts: http://gtfs.edmonton.ca/TMGTFSRealTimeWebService/Alert/Alerts.pb
    Documentation: https://www.transit.land/feeds/f-ets~rt

    Example Code: https://nbviewer.org/url/nikhilvj.co.in/files/gtfsrt/locations.ipynb
    Travel Time: https://docs.traveltime.com/api/overview/introduction
    MBTA: https://github.com/mbta/gtfs-documentation/blob/master/reference/gtfs-realtime.md
    """

    dict_obj = read_pb(
        'http://gtfs.edmonton.ca/TMGTFSRealTimeWebService/TripUpdate/TripUpdates.pb')
    lastUpdate = dict_obj["header"]["timestamp"]
    print(f'Last Update: {convert_posix_to_mst(int(lastUpdate))}')
    # print(json.dumps(dict_obj["entity"][0],indent=4))

    master = []

    for entity in dict_obj["entity"]:
        eid = entity["id"]
        routeid = entity["tripUpdate"]["trip"]["routeId"]

        for stop in entity["tripUpdate"]["stopTimeUpdate"][:4]:
            # Grab info from first 4 stops from list
            stopid = stop["stopId"]

            delay = 0
            departure = stop.get("departure", None)
            if departure:
                delay = departure.get("delay", 0)
            master.append(create_dict(eid, routeid, stopid, delay, lastUpdate))

    # print(json.dumps(master, indent=4))

    # write_to_json(master, 'master')
    # write_csv_from_dict_list(master, 'master_converted')

    # write_to_json(dict_obj, 'hum')

    append_dict_list_to_csv(master, 'data/master_converted.txt')

    remove_duplicates_from_csv('data/master_converted.txt')


if __name__ == '__main__':

    max_iterations = 1
    iteration_count = 0
    while iteration_count < max_iterations:
        main()
        iteration_count += 1
        time.sleep(180)

    print(f"Script completed after {iteration_count}")
