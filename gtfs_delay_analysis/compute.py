from typing import Callable
import polars as pl
import os


def load_or_compute(file: str, func: Callable[[], pl.DataFrame]):
    if not os.path.exists(file):
        df = func()
        df.write_parquet(file)
        return df
    else:
        return pl.read_parquet(file)
