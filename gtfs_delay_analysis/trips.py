import polars as pl

from .compute import load_or_compute


def load_trips_lazy(path='data/ETS_Bus_Schedule_GTFS_Data_Feed_-_Trips_20240321.csv'):
    return pl.scan_csv(path, infer_schema_length=10000)


def load_str_shapes_df(trips: pl.LazyFrame | None = None):
    return load_or_compute(
        'data/shapes.parquet',
        lambda: (
            (trips or load_trips_lazy())
            .select('shape_id', 'line_length', 'geometry_line')
            .unique().collect()
        ),
    )


def load_parsed_shapes_df(shapes: pl.DataFrame | None = None):
    return load_or_compute(
        'data/shapes-parsed.parquet',
        lambda: (
            (shapes or load_str_shapes_df())
            .with_columns(
                pl.col('geometry_line')
                .str.slice(len("MULTILINESTRING (("))
                .str.strip_chars_end(')')
                .str.split(', ')
                .list.eval(
                    pl.element()
                    .str.split(' ')
                    .list.eval(pl.element().cast(pl.Float64))
                    .list.to_struct(fields=['lon', 'lat'])
                )
            )
        )
    )


def load_trips_without_shapes_df(trips: pl.LazyFrame | None = None):
    def load(df: pl.LazyFrame):
        cols = [*df.columns]
        cols.remove('geometry_line')
        cols.remove('line_length')
        return df.select(cols).collect()
    return load_or_compute(
        'data/trips-no-shapes.parquet',
        lambda: load(trips or load_trips_lazy())
    )
