from typing import Optional

import polars as pl
import seaborn as sb


def get_ddelay(df: pl.DataFrame):
    return (
        df
        .with_columns(
            pl.col('meandelay').diff().alias('ddelay'),
            pl.col('index').sub(1).alias('prev')
        )
        .join(
            df.select('stopid', 'index', 'stop_lon', 'stop_lat', 'trip_id'),
            left_on=['prev', 'trip_id'],
            right_on=['index', 'trip_id'],
        )
        .select(
            a_id='stopid_right',
            b_id='stopid',
            a_lon='stop_lon_right',
            a_lat='stop_lat_right',
            b_lon='stop_lon',
            b_lat='stop_lat',
            shapeid='shape_id',
            trip_id='trip_id',
            ddelay='ddelay',
        )
        .with_columns(line=pl.concat_str('a_id', 'b_id', separator='-'))
        .select('line', 'ddelay', 'trip_id')
    )


def plot_ddelay(df: pl.DataFrame, title: Optional[str] = None):
    ax = sb.barplot(df, x='line', y='ddelay')
    ax.set_xticks(ax.get_xticks(), df['line'], rotation=60)
    if title:
        ax.set_title(title)


def plot_mean(df: pl.DataFrame):
    ax = sb.lineplot(df, x='stopid', y='meandelay')
    ax.set_xticks(ax.get_xticks(), df['stopid'], rotation=60)
