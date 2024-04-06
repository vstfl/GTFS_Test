from typing import Optional

import polars as pl

from .trips import load_parsed_shapes_df, load_trips_without_shapes_df


def join_stops(df: pl.DataFrame, stops: pl.DataFrame):
    return (
        df.with_columns(pl.col('stopid').cast(pl.Utf8))
        .join(stops, left_on='stopid', right_on='stop_id')
    )


def make_sequence(
    df: pl.DataFrame,
    trip_id: Optional[int] = None,
    shape_id: Optional[str] = None,
    trips: Optional[pl.DataFrame] = None,
    shapes: Optional[pl.DataFrame] = None,
):
    trips = trips if trips is not None else load_trips_without_shapes_df()
    shapes = shapes if shapes is not None else load_parsed_shapes_df()
    by_stop = df.group_by('stopid').agg(
        pl.col('meandelay').mean(),
        pl.col('stop_lon').first(),
        pl.col('stop_lat').first(),
        pl.col('routeid').first().cast(pl.Utf8),
    )
    pred = (
        pl.col('trip_id').eq(trip_id) if trip_id
        else pl.col('shape_id').eq(shape_id) if shape_id
        else True)
    trip_points = (
        trips
        .filter(pred)
        .join(shapes, on='shape_id')
        .unique('shape_id')
        .explode('geometry_line')
        .unique('geometry_line', keep='first', maintain_order=True)
        .with_row_index()
    )
    return (
        trip_points.join(
            by_stop,
            left_on='route_id',
            right_on='routeid',
        )
        .with_columns(
            pl.col('geometry_line').struct.field(
                'lon').sub(pl.col('stop_lon')),
            pl.col('geometry_line').struct.field(
                'lat').sub(pl.col('stop_lat')),
        )
        # Get euclidean distance
        .with_columns(
            pl.col('lon').pow(2).add(pl.col('lat').pow(2))
            .sqrt().alias('euclidean')
        )
        # Get the minimum euclidean distance for a stop
        .filter(pl.col('euclidean').eq(pl.col('euclidean').min().over('stopid')))
        # Re-create index
        .sort('index')
        .drop('index')
        .with_row_index()
        .select([
            'index',
            'route_id',
            'trip_headsign',
            'stopid',
            'shape_id',
            'meandelay',
            'stop_lon',
            'stop_lat',
        ])
    )
