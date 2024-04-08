from typing import Optional

import polars as pl

from .trips import load_trips_without_shapes_df


def join_stops(df: pl.DataFrame, stops: pl.DataFrame):
    return (
        df.with_columns(pl.col('stopid').cast(pl.Utf8))
        .join(stops, left_on='stopid', right_on='stop_id')
    )


def make_sequence(
    df: pl.DataFrame,
    trips: Optional[pl.DataFrame] = None,
    sequences: Optional[pl.DataFrame] = None,
):
    trips = trips if trips is not None else load_trips_without_shapes_df()
    seq = (
        sequences if sequences is not None
        else pl.read_parquet('stop-sequences.parquet')
    )
    return (
        df.group_by('id', 'stopid').agg(
            pl.col('meandelay').mean(),
            pl.col('stop_lon').first(),
            pl.col('stop_lat').first(),
            pl.col('routeid').first().cast(pl.Utf8),
        )
        .join(trips, left_on='id', right_on='trip_id', how='left')
        .join(
            seq.with_columns(pl.col('stop_id').cast(pl.Utf8)),
            left_on=['id', 'stopid'],
            right_on=['trip_id', 'stop_id'],
            how='left',
        )
        .select(
            'id',
            'stop_sequence',
            'route_id',
            'trip_headsign',
            'stopid',
            'shape_id',
            'meandelay',
            'stop_lon',
            'stop_lat',
        )
    )
