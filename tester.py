import pandas as pd


def check_duplicates(csv_file_path):
    try:
        # Read the CSV file into a DataFrame with explicit header
        df = pd.read_csv(csv_file_path)

        # Get the number of rows before checking for duplicates
        initial_row_count = len(df)

        # Check for duplicates based on all columns
        duplicate_rows = df[df.duplicated(subset=df.columns)]

        # Count the number of duplicates
        duplicate_count = len(duplicate_rows)

        if duplicate_count > 0:
            print(
                f'{duplicate_count} duplicates found in CSV file "{csv_file_path}".')
            print('Duplicate Rows:')
            print(duplicate_rows)
        else:
            print(f'No duplicates found in CSV file "{csv_file_path}".')

        return duplicate_count

    except FileNotFoundError:
        print(f"File '{csv_file_path}' not found.")
        return 0  # No duplicates checked since file not found


if __name__ == '__main__':

    csv_file_path = 'data/final_csv.csv'  # Replace with your CSV file path
    check_duplicates(csv_file_path)
