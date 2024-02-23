"""
Example of writing dataframe to database
"""
import sqlite3
import pandas as pd

if __name__ == "__main__":
    db_path = "data/gtfs.db"
    df = pd.read_csv('data/master_converted.csv')
    with sqlite3.connect(db_path) as con:
        df.to_sql(
            name='stops',  # Name of database table
            con=con,  # SQLite connection
            if_exists='append',  # Append to table if it already exists
            index=False,  # Removes Pandas index column (bloat)
        )
