"""
Example of reading dataframe from database
"""
import sqlite3
import pandas as pd

if __name__ == "__main__":
    db_path = "data/gtfs.db"
    with sqlite3.connect(db_path) as con:
        df = pd.read_sql('select * from stops', con=con)
        print(df)
