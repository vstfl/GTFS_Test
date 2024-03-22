import polars as pl
from glob import glob
import seaborn as sb

from .compute import load_or_compute

from . import trips
from . import compute


def get_dfs_from_glob(glob_str: str):
    dfs = [
        pl.read_csv(i, dtypes={'stopid': pl.Utf8, 'routeid': pl.Utf8})
        for i in glob(glob_str)
    ]
    return pl.concat(dfs, how='vertical').unique()


def load_raw_data():
    """
    Load raw data from existing parquet file or from CSVs.
    """
    # Loading from the CSVs take like 30 seconds!!!
    # Save into a more compact form for easier retrieval later

    return load_or_compute(
        'raw.parquet',
        lambda: get_dfs_from_glob('raw_data/raw_trip_*').unique()
    ).cast({
        # Save memory by re-using strings for categorical data
        'period': pl.Categorical,
        'routeid': pl.Categorical,
        'stopid': pl.Categorical
    })


def load_aggregate_data():
    return load_or_compute(
        'aggregated.parquet',
        lambda: aggregate_raw_data(load_raw_data())
    )


def add_coords(df: pl.DataFrame, stops: pl.DataFrame):
    return (
        df.with_columns(pl.col('stopid').cast(pl.Utf8))
        .join(stops.select(lat='stop_lat', lon='stop_lon', stopid='stop_id'), on='stopid', how='left')
    )


def aggregate_raw_data(df: pl.DataFrame):
    # Aggregate all data
    delay = pl.col('delay')
    return (
        df.with_columns(delay.abs())
        .with_columns(
            pl.from_epoch('lastupdate', time_unit='s')
            # MST
            .dt.offset_by('-7h')
        )
        .with_columns(
            pl.col('lastupdate').dt.date().alias('date'),
            pl.col('lastupdate').dt.weekday().alias('day'),
        )
        # Get only the weekdays (6 = saturday, 7 = sunday)
        .filter(pl.col('day') < 6)
        # Remove snowstorm stuff
        .filter(pl.col('date').cast(pl.Utf8) > '2024-03-03')
        .sort('lastupdate')
        .group_by('id', 'stopid', 'date', 'period')
        .agg(
            pl.col('routeid').first(),
            pl.col('lastupdate').max(),
            delay.len().alias('count'),
            delay.max().alias('maxdelay'),
            delay.mean().alias('meandelay'),
            delay.median().alias('mediandelay'),
            delay.std().alias('stddelay'),
            delay
        )
        .with_columns(pl.col('date').dt.weekday().alias('day'))
        .with_columns(pl.col('lastupdate').dt.hour().alias('hour'))
        # Just remove trips that are above MAX_DELAY
        .filter(pl.col('meandelay') < MAX_DELAY)
    )


MAX_DELAY = 60*20


def plot_stop(dfs: pl.DataFrame, stopid: str, max_delay=MAX_DELAY):
    cats = (
        dfs
        .filter(pl.col('delay').abs() < max_delay)
        .filter(pl.col('stopid') == stopid)
        .group_by('id')
        .all()
        .select('id', 'delay', 'lastupdate')
        .with_columns(pl.col('delay').list.len().alias('length'))
        .explode('delay', 'lastupdate')
        .sort('id', 'lastupdate')
        .with_columns(pl.col('id').cast(pl.Utf8).cast(pl.Categorical))
    )
    sb.scatterplot(cats, x='lastupdate', y='delay', hue='id')


def agg_group(df: pl.DataFrame, *groups: str):
    return (
        df.group_by(groups)
        .agg(
            pl.col('meandelay').mean().alias('avgdelay'),
            pl.col('meandelay').max().alias('maxdelay'),
            pl.col('id').len().alias('numtrips'),
            pl.col('count').sum(),
            pl.col('lastupdate')
        )
        .sort(groups)
    )


def select_stop(df: pl.DataFrame, stopid: str):
    return (
        df.filter(pl.col('stopid') == stopid)
        .pipe(agg_group, 'routeid')
        .drop('lastupdate')
    )


def select_stop_and_route(df: pl.DataFrame, stopid: str, routeid: str):
    return (
        df.filter(
            (pl.col('stopid') == stopid) &
            (pl.col('routeid') == routeid)
        )
        .pipe(agg_group, 'day', 'hour')
        .drop('lastupdate')
    )


def load_trips_df(path='data/trips.json', pq_path='data/trips.parquet'):
    def load():
        import json
        return (
            pl.DataFrame(json.load(open(path)))
            # Trim JSON data according to the assumptions
            .with_columns(pl.col('geometry_line').struct.field('coordinates'))
            .with_columns(pl.col('coordinates').list.get(0))
            .drop('geometry_line')
        )
    return load_or_compute(pq_path, load)
