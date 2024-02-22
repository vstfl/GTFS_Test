import pandas as pd

"""
Main file for querying/sorting data to be used in GIS software

Objectives: Develop the functions:
1. Take all rows of same stopid at a given time interval, take an absolute average of all delays at stop,
bundle with co-ords and then output [DONE]
- each stopid should be unique in final file

2. Take all rows of same stopid at a given time interval, bundle co-ords, average delay, and id, then output
- each stopid can have multiple entries corresponding to each trip (id)
- not exactly required, probably can implement within tableau

3. Develop a function that removes all rows not within time range [DONE]
4. Develop a function that takes associated stopid and bundles with a co-ordinate [DONE]

# Can use this data to connect what causes delays to factors i.e.:
- Proximity to schools, demand in area, road density
- Time of day, Time of week
- Weather variables (precipitation counts, etc)

"""


def filter_dataframe_by_time(df, start_times, time_range, date_list):
    filtered_rows = []

    for start_time in start_times:
        # Convert start_time to minutes
        start_time_minutes = start_time * 60

        # Iterate over each date in the date_list
        for date in date_list:
            # Create a datetime column combining date and lastupdate
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')

            # Filter rows for the specific date
            date_rows = df[df['datetime'].dt.date == pd.to_datetime(date).date()]

            # Filter rows within the specified time range
            time_range_rows = date_rows[
                (date_rows['datetime'].dt.hour * 60 + date_rows['datetime'].dt.minute >= start_time_minutes) &
                (date_rows['datetime'].dt.hour * 60 + date_rows['datetime'].dt.minute < start_time_minutes + time_range)
            ]

            # Append the filtered rows to the list
            filtered_rows.append(time_range_rows)

    # Concatenate the filtered rows into a new DataFrame
    result_df = pd.concat(filtered_rows, ignore_index=True)

    # Drop the temporary 'datetime' column
    result_df.drop(columns=['datetime'], inplace=True, errors='ignore')

    return result_df


def aggregate_stop_data(input_df):
    # Ensure 'timestamp' is in datetime format
    input_df['timestamp'] = pd.to_datetime(input_df['timestamp'], unit='s')

    # Take the absolute value of 'averagedelay'
    input_df['averagedelay_abs'] = input_df['averagedelay'].abs()

    # Group by 'stopid' and calculate the average absolute delay, average timestamp, and count of entries
    aggregated_df = input_df.groupby('stopid').agg({
        'averagedelay_abs': 'mean',
        'timestamp': 'mean',
        'id': 'count'  # Use a different name for the count column, e.g., 'entry_count'
    }).reset_index()

    # Convert 'timestamp' back to Unix timestamp for consistency
    aggregated_df['timestamp'] = aggregated_df['timestamp'].astype('int64') // 10**9

    # Rename the count column to 'entry_count'
    aggregated_df.rename(columns={'id': 'entry_count'}, inplace=True)

    return aggregated_df


def merge_coordinates(df1, df2):
    # Ensure the 'stopid' and 'stop_id' columns are of the same type
    df1['stopid'] = df1['stopid'].astype(str)
    df2['stop_id'] = df2['stop_id'].astype(str)

    # Merge the two DataFrames on 'stopid' and 'stop_id'
    merged_df = pd.merge(df1, df2, left_on='stopid', right_on='stop_id', how='left')

    # Create a duplicate of the original DataFrame
    result_df = df1.copy()

    # Add 'latitude' and 'longitude' columns using the data from the second DataFrame
    result_df['latitude'] = merged_df['stop_lat']
    result_df['longitude'] = merged_df['stop_lon']

    return result_df


def export_to_csv(df, file_path):
    df.to_csv(file_path, index=False)
    print(f'DataFrame has been exported to {file_path}')


def main():
    file_path = 'final_csv.txt'

    df = pd.read_csv(file_path, delimiter=',')
    bus_stops = pd.read_csv('ETS_Bus_Schedule_GTFS_Data_Feed_-_Stops_20240216.csv')

    start_time = [8]  # 8:30 AM
    time_range = 1440   # 45 minutes
    date_list = ['2024-02-14', '2024-02-15']

    filtered_df = filter_dataframe_by_time(df, start_time, time_range, date_list)
    overview_df = aggregate_stop_data(filtered_df)
    overview_w_coords = merge_coordinates(overview_df, bus_stops)

    print(filtered_df)
    #print(overview_w_coords)

    export_to_csv(filtered_df, 'TIMEFILTER.csv')

    #export_to_csv(overview_w_coords, 'OVERALL_TEST.csv')


if __name__ == '__main__':
    main()
