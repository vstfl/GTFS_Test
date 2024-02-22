import pandas as pd
from collections import defaultdict
import json


def create_dict_from_csv(csv_file_path):
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(csv_file_path)

        # Create a dictionary from the DataFrame
        dict_obj = {}
        for index, row in df.iterrows():
            stopid = row['stopid']
            routeid = row['routeid']
            delay = row['delay']
            id_value = row['id']
            lastupdate = row['lastupdate']

            if stopid not in dict_obj:
                dict_obj[stopid] = []

            dict_obj[stopid].append({
                'routeid': routeid,
                'delay': delay,
                'id': id_value,
                'lastupdate': lastupdate
            })

        return dict_obj

    except FileNotFoundError:
        print(f"File '{csv_file_path}' not found.")
        return None


def export_dict_to_json(dict_obj, output_json_file):
    if dict_obj is None:
        print("Cannot export, dictionary is None.")
        return

    try:
        # Convert the dictionary to a JSON string
        json_data = json.dumps(dict_obj, indent=2)

        # Write the JSON string to a new text file
        with open(output_json_file, 'w') as json_file:
            json_file.write(json_data)

        print(f'Dictionary exported to JSON file "{output_json_file}" successfully.')

    except Exception as e:
        print(f"An error occurred during export: {e}")


def export_dict_to_csv(dict_obj, output_csv_file):
    if dict_obj is None:
        print("Cannot export, dictionary is None.")
        return

    try:
        # Convert the dictionary to a DataFrame
        result_df = pd.DataFrame.from_dict({(i, j): dict_obj[i][j]
                                            for i in dict_obj.keys()
                                            for j in range(len(dict_obj[i]))}, orient='index')

        # Export the DataFrame to a new CSV file
        result_df.to_csv(output_csv_file, index=False)

        print(f'Dictionary exported to CSV file "{output_csv_file}" successfully.')

    except Exception as e:
        print(f"An error occurred during export: {e}")


def convert_dict_to_csv(dict_obj, output_csv_file):
    try:
        # Create an empty list to store rows
        rows = []

        # Iterate through each bus stop in the dictionary
        for stopid, entries in dict_obj.items():
            # Iterate through each entry in the bus stop
            for entry in entries:
                # Create a dictionary for each row
                row = {
                    'stopid': stopid,
                    'routeid': entry['routeid'],
                    'averagedelay': entry['averagedelay'],
                    'id': entry['id'],
                    'timestamp': entry['timestamp']
                }
                # Append the row to the list
                rows.append(row)

        # Convert the list of dictionaries to a DataFrame
        df = pd.DataFrame(rows)

        # Export the DataFrame to a CSV file
        df.to_csv(output_csv_file, index=False)

        print(f'Dictionary exported to CSV file "{output_csv_file}" successfully.')

    except Exception as e:
        print(f"An error occurred during export: {e}")


def average_delay_by_id2(dict_obj):
    if dict_obj is None:
        print("Cannot average delay, dictionary is None.")
        return None

    # Create a defaultdict to store aggregated data
    aggregated_dict = defaultdict(list)

    # Variables to track size before and after the function
    original_size = sum(len(entries) for entries in dict_obj.values())
    new_size = 0

    # Iterate through each bus stop in the dictionary
    for stopid, entries in dict_obj.items():
        id_values = set()  # To track unique "id" values for the current bus stop
        id_delay_sum = defaultdict(float)  # To store the sum of "delay" for each "id"
        id_count = defaultdict(int)  # To store the count of entries for each "id"

        unique_entries = []
        # Iterate through each entry in the bus stop
        for entry in entries:
            id_value = entry['id']
            delay = entry['delay']

            # Check if the current "id" is already encountered for this bus stop
            if id_value in id_values:
                # If yes, accumulate the "delay" and increase the count for averaging
                id_delay_sum[id_value] += delay
                id_count[id_value] += 1
            else:
                # If not, add the "id" to the set and initialize the "delay" sum and count
                id_values.add(id_value)
                id_delay_sum[id_value] = delay
                id_count[id_value] = 1

        # Calculate the average "delay" for each "id" at the current bus stop
        average_delays = {id_value: id_delay_sum[id_value] / id_count[id_value] for id_value in id_values}

        # Create a new entry in the aggregated dictionary for the current bus stop
        aggregated_dict[stopid] = [{
            'routeid': [entry['routeid'] for entry in entries if entry['id'] == entry_id][0],
            'averagedelay': average_delays[entry_id],
            'id': entry_id,
            'timestamp': [entry['lastupdate'] for entry in entries if entry['id'] == entry_id][0],
        } for entry_id in id_values]

        # Update the new size
        new_size += len(aggregated_dict[stopid])

    print(f"Original size of list of dicts: {original_size}")
    print(f"New size of list of dicts after averaging: {new_size}")

    return dict(aggregated_dict)


def main():
    """
    Generate files containing delay data for each route at each stop

    Input file must be a master list containing id,routeid,stopid,delay,lastupdate
    input file can and should include as much data as possible (i.e. 1 whole day, or 1 whole week)
    """
    obj = create_dict_from_csv('master_converted.txt')
    #export_dict_to_csv(obj,'test_dict.txt')
    aggregated_result = average_delay_by_id2(obj)
    #export_dict_to_json(aggregated_result, 'test_dict.txt')
    convert_dict_to_csv(aggregated_result, 'final_csv.txt')


if __name__ == '__main__':
    main()
