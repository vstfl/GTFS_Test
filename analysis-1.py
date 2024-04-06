import polars as pl
import seaborn as sb
import gtfs_delay_analysis as da

aggregated = da.load_aggregate_data()
stops = pl.read_csv(
    '/home/chrlz/dox/dl/ETS_Bus_Schedule_GTFS_Data_Feed_-_Stops_20240216.csv')

"""
So we know that all trip id is unique for a single day, no need to worry about overlaps

Average delay in a stop every 3 minutes 10 recordings of a bus

AM: 7am-9am
PM: 4pm-7pm
OFF: 5am-7am, 9am-4-pm, 7pm-10pm
"""

# 1. Map delay average over week
mean_stop = aggregated.pipe(da.agg_group, 'stopid')

# 2. Over week by day
# TODO: Filter by PEAK, OFF, and DAY


def pivot_day(df: pl.DataFrame):
    def get_day(d: str):
        if 'day' not in d:
            return d
        col, _, d = d.split('_')
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
        return f'{days[int(d)-1]}_{col[:3]}'
    return (
        df.select('day', 'hour', 'avgdelay', 'stddelay')
        .pivot(values=['avgdelay', 'stddelay'], index='hour', columns='day')
        .rename(get_day)
    )


def filter_by_stop_and_route(agg: pl.DataFrame, stopid: str, routeid: str):
    g_day, g_hour, g_hour_pivot = (
        agg
        .filter(
            (pl.col('stopid') == stopid)
            & (pl.col('routeid') == routeid)
        )
        .pipe(by_day)
    )

    g_hour.drop('lastupdate').write_csv(f'{stopid}-{routeid}-by-hour.csv')
    g_hour_pivot.write_csv(f'{stopid}-{routeid}-by-hour-pivot.csv')
    g_day.write_csv(f'{stopid}-{routeid}-by-day.csv')
    return g_day, g_hour


def by_day(df: pl.DataFrame):
    by_day_all = df.pipe(da.agg_group, 'day')
    by_day_peak = (
        df
        .filter(pl.col('period') != 'OFF')
        .pipe(da.agg_group, 'day')
    )
    by_day_off = (
        df
        .filter(pl.col('period') == 'OFF')
        .pipe(da.agg_group, 'day')
    )

    def get(df: pl.DataFrame, prefix: str):
        return df.select(
            'day',
            pl.col('avgdelay').alias(f'{prefix}_avg'),
            pl.col('stddelay').alias(f'{prefix}_std'),
        )
    g_day = (
        by_day_all.pipe(get, 'all')
        .join(by_day_peak.pipe(get, 'peak'), on='day', how='left')
        .join(by_day_off.pipe(get, 'off'), on='day', how='left')
    )

    # 3. Over day by hour
    by_hour = df.pipe(da.agg_group, 'day', 'hour')
    return g_day, by_hour, by_hour.pipe(pivot_day)


network_by_day, by_hour, by_hour_pivot = by_day(aggregated)

# TODO: Select route
# 4. Time series graph of route by hour by day


highest_delay_stops = (
    mean_stop
    .group_by('stopid')
    .agg(pl.col('avgdelay').max())
    .sort('avgdelay', descending=True)
    .select('stopid')
    .head(100)
    .unique()
    .join(aggregated, on='stopid')
)
routes_on_highest_delay_stops = (
    highest_delay_stops
    .select('routeid')
    .unique()
    .join(aggregated, on='routeid')
)
agg_routes_on_highest_delay = (
    routes_on_highest_delay_stops
    .pipe(da.agg_group, 'routeid')
    .drop('lastupdate')
)

(
    mean_stop.drop('lastupdate')
    .pipe(da.add_coords, stops)
    .write_csv('mean-stop.csv')
)


def plot_cols(df: pl.DataFrame, key: str):
    tidy = df.melt(id_vars=key).sort(key)
    xticks = tidy[key]
    ax = sb.barplot(
        data=tidy,
        x=key,
        y='value',
        hue='variable',
        width=1,
        order=xticks,
    )
    return tidy, ax


def plot_by_route(df: pl.DataFrame):
    tidy, ax = plot_cols(df, 'routeid')
    xticks_unique = tidy['routeid'].unique()
    ax.set_xticks(ax.get_xticks(), xticks_unique, rotation=90)


# 5. Mapping delay propagations within a route
# 6. TODO

def plot_stop_and_route(df: pl.DataFrame, stop: str, route: str):
    days = pl.DataFrame({
        "day": [*range(1, 6)],
        "letter": ['M', 'T', 'W', 'R', 'F'],
    }, schema={"day": pl.Int8, 'letter': pl.Categorical})
    by_day_by_hour_for_stop = (
        da.select_stop_and_route(df, stop, route)
        .join(days, on='day')
        .drop('day')
        .rename({'letter': 'day'})
    )

    ax = sb.barplot(by_day_by_hour_for_stop, x='hour', y='avgdelay', hue='day')
    ax.set_title(f'Average delay for route {route} stop {stop}')
    return by_day_by_hour_for_stop


aggregated.filter(pl.col('stopid') == '1899').group_by('routeid').agg(
    pl.col('meandelay').mean().alias('avgdelay'),
    pl.col('maxdelay').max().alias('maxdelay'),
    pl.col('count').sum()
).write_csv('1899-by-route.csv')

# select_stop(aggregated, "2260")

# sb.barplot(by_day_by_hour_for_stop, x='hour', y='maxdelay')


# highest_delay_stops.select('stopid').unique()

# plot_stop_and_route(aggregated, "2260", "637")
# da.select_stop(aggregated, "2260")
mean_stop.pipe(da.add_coords, stops).drop(
    'lastupdate').write_csv('mean-stop.csv')

network_by_day.drop('lastupdate').write_csv('network-by-day.csv')
by_hour.drop('lastupdate').write_csv('network-by-day-by-hour.csv')
# by_hour.drop('lastupdate')

to_plot = by_hour.select('day', 'hour', 'avgdelay')
ax = sb.lineplot(to_plot, x='hour', y='avgdelay', hue='day')
ax.set_xticks([*range(5, 23)]) and None

*_, network_pivot = aggregated.pipe(by_day)

aggregated.filter((pl.col('stopid') == "2260")
                  & (pl.col('routeid') == "637")
                  )

_, bd = aggregated.pipe(filter_by_stop_and_route, '2260', '004')
_, bd = aggregated.pipe(filter_by_stop_and_route, '2260', '637')
m637 = aggregated.filter(
    pl.col('routeid').__eq__('637').and_(
        pl.col('stopid').__eq__('2260')
    )
)['meandelay'].mean()

m004 = aggregated.filter(
    pl.col('routeid').__eq__('004').and_(
        pl.col('stopid').__eq__('2260')
    )
)['meandelay'].mean()
m637, m004

# plot_stop_and_route(aggregated, '1899', '560')
# plot_stop_and_route(aggregated, '1899', '413')
# plot_stop_and_route(aggregated, '5281', '002')
# plot_stop_and_route(aggregated, '5281', '004')

aggregated.filter(
    (pl.col('stopid') == '2260')
    # & (pl.col('routeid') == '904')
).group_by('routeid').agg(
    pl.col('count').sum(),
    pl.col('meandelay').sum(),
)
# 25529677

da.select_stop(aggregated, '2260')

# plot_stop_and_route(aggregated, '2260', '004')
plot_stop_and_route(aggregated, '2260', '637')

# Find all points for that trip


def filter_by_trip(id: int, day: int):
    df = aggregated.filter(
        (pl.col('id') == id) &
        (pl.col('day') == day)
    ).pipe(da.add_coords, stops).sort('meandelay').drop('delay')
    df.write_csv(f'{id}-{day}-stops.csv')
    return df


# filter_by_trip(25527827, 2)
# filter_by_trip(25527777, 2)
# filter_by_trip(25527796, 2)


most_delayed_trips = [
    *aggregated
    .filter(pl.col('routeid') == "637")
    .group_by('id', 'date', 'day')
    .agg(pl.col('meandelay').max())
    .sort('meandelay', descending=True)
    .head(3)
    .select('id', 'day')
    .iter_rows()
]

trips = da.trips.load_trips_without_shapes_df()
shapes = da.trips.load_str_shapes_df()

(
    trips.filter(pl.col('trip_id') == 25536739)
    .join(shapes, on='shape_id')
    .select('route_id', 'shape_id')
    .unique()
    .join(shapes, on='shape_id')
    .write_csv(f'004-{25536739}-shapes.csv')
)

# Find trip with highest delay on Tuesday for route 560
(

    aggregated.filter(
        pl.col('routeid').eq("560").and_(pl.col('day').eq(2))
    )
    .group_by('id')
    .agg(pl.col('meandelay').max())
    .sort('meandelay', descending=True)
)


tr = (
    trips
    .filter(pl.col('shape_id') == "004-168-West")
    .join(shapes, on='shape_id')
    .explode('geometry_line')
    .with_columns(
        pl.col('geometry_line').struct.field('lon'),
        pl.col('geometry_line').struct.field('lat')
    )
)

pl.Config.set_fmt_str_lengths(100)

(
    aggregated.join(
        trips.filter(pl.col('shape_id') ==
                     "004-168-West").select(id='trip_id'),
        on='id'
    )
    .with_columns(pl.col('stopid').cast(pl.Utf8))
    .join(
        stops,
        left_on="stopid",
        right_on="stop_id"
    )
    .with_columns(
        pl.col('geometry_point')
        .str.strip_prefix("POINT (")
        .str.strip_suffix(")")
        .str.split(" ")
        .list.eval(pl.element().cast(pl.Float64))
        .list.to_struct(fields=["lon", "lat"])
    ).with_columns(
        pl.col('geometry_point').struct.field('lon'),
        pl.col('geometry_point').struct.field('lat')
    )
    .join(tr, left_on='id', right_on="trip_id")
    .with_columns(
        pl.col('lon')
        .sub(pl.col('lon_right'))
        .abs()
        .add(pl.col('lat').sub(pl.col('lat_right')).abs())
        .alias('manhattan')
    )
    .filter(pl.col('manhattan') == pl.col('manhattan').min().over('stopid'))
    # .sort('id')
    # .filter()
)

(
    aggregated
    .join(
        trips
        .filter(pl.col('shape_id').eq("004-168-West"))
        .select(id='trip_id'),
        on='id'
    )
    .group_by('stopid')
    .all()
)


(
    shapes
    .with_columns(pl.col('geometry_line').list.len().alias('point_count'))
    .sort('point_count')
    ['point_count'].sum()
)
