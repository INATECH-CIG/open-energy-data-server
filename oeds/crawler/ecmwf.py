# SPDX-FileCopyrightText: Jonathan Sejdija, Florian Maurer
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import csv
import logging
import shutil
import zipfile
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path

import cdsapi
import geopandas as gpd
import pandas as pd
import swifter  # noqa: F401
import xarray as xr
from shapely.geometry import Point
from sqlalchemy import text

from oeds.base_crawler import (
    DEFAULT_CONFIG_LOCATION,
    ContinuousCrawler,
    CrawlerConfig,
    load_config,
)

##################
# IMPORTANT NOTE #
##################
# in order for this crawler to run, you need to run the 'public' crawler first
# see readme
# https://github.com/open-energy-data-server/open-energy-data-server/blob/main/README.md#using-the-ecmwf-crawler

"""
    Note that only requests with no more that 1000 items at a time are valid.
    See the following link for further information:
    https://confluence.ecmwf.int/pages/viewpage.action?pageId=308052947

    Also "Surface net solar radiation" got renamed to "Surface net short-wave (solar) radiation"
"""

log = logging.getLogger("ecmwf")

# path of nuts file
# downloaded from
# https://ec.europa.eu/eurostat/web/gisco/geodata/statistical-units/territorial-units-statistics
NUTS_PATH = Path(__file__).parent.parent / "shapes/NUTS_RG_01M_2021_4326.shp"
TEMP_DIR = Path(__file__).parent.parent / "ecmwf_grb_files"

# coords for europe according to:
# https://web.archive.org/web/20240817225744/https://cds.climate.copernicus.eu/toolbox/doc/how-to/1_how_to_retrieve_data/1_how_to_retrieve_data.html#retrieve-a-geographical-subset-and-change-the-default-resolution
coords = [75, -15, 30, 42.5]

# requested weather variable
weather_variables = [
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "2m_temperature",
    "total_precipitation ",
    "surface_net_solar_radiation",
]

TEMPORAL_START = datetime(2022, 1, 1)


def create_table(engine):
    with engine.begin() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ecmwf( "
            "time timestamp without time zone NOT NULL, "
            "temp_air double precision, "
            "ghi double precision, "
            "wind_meridional double precision, "
            "wind_zonal double precision, "
            "wind_speed double precision, "
            "precipitation double precision, "
            "latitude double precision, "
            "longitude double precision, "
            "PRIMARY KEY (time , latitude, longitude));"
        )

    with engine.begin() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ecmwf_eu( "
            "time timestamp without time zone NOT NULL, "
            "temp_air double precision, "
            "ghi double precision, "
            "wind_meridional double precision, "
            "wind_zonal double precision, "
            "wind_speed double precision, "
            "precipitation double precision, "
            "latitude double precision, "
            "longitude double precision, "
            "nuts_id text, "
            "PRIMARY KEY (time , latitude, longitude, nuts_id));"
        )


def save_ecmwf_request_to_file(request, ecmwf_client: cdsapi.Client):
    # path for downloaded files from copernicus
    filename = f"{request['year']}_{request['month']}_{request['day'][0]}-{request['month']}_{request['day'][-1]}_ecmwf.zip"
    save_downloaded_files_path = TEMP_DIR / filename
    ecmwf_client.retrieve("reanalysis-era5-land", request, save_downloaded_files_path)


def psql_insert_copy(table, conn, keys: list[str], data_iter):
    # gets a DBAPI connection that can provide a cursor
    with conn.connection.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ", ".join(f'"{k}"' for k in keys)
        if table.schema:
            table_name = f"{table.schema}.{table.name}"
        else:
            table_name = table.name

        sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV"
        cur.copy_expert(sql=sql, file=s_buf)


def build_dataframe(engine, request: dict, write_lat_lon: bool = True):
    filename = f"{request['year']}_{request['month']}_{request['day'][0]}-{request['month']}_{request['day'][-1]}_ecmwf.zip"
    file_path = TEMP_DIR / filename
    filename = "2022_01_0-01_1_ecmwf.grb"

    if zipfile.is_zipfile(file_path):
        # log.info("extracting zipfile %s", file_path)
        with zipfile.ZipFile(file_path) as z_file:
            z_file.extractall(TEMP_DIR / (filename + ".dir"))
        file = TEMP_DIR / (filename + ".dir") / "data.grib"
        weather_data = xr.open_dataset(file, engine="cfgrib")
    else:
        weather_data = xr.open_dataset(file_path, engine="cfgrib", indexpath="")
    log.info(f"successfully read file {file_path}")
    weather_data = weather_data.to_dataframe()
    weather_data = weather_data.dropna(axis=0)
    weather_data = weather_data.reset_index()
    weather_data = weather_data.drop(
        ["time", "step", "number", "surface"], axis="columns"
    )
    weather_data = weather_data.rename(
        columns={
            "valid_time": "time",
            "u10": "wind_zonal",
            "v10": "wind_meridional",
            "t2m": "temp_air",
            "ssr": "ghi",
            "tp": "precipitation",
        }
    )
    # calculate wind speed from zonal and meridional wind
    weather_data["wind_speed"] = (
        weather_data["wind_zonal"] ** 2 + weather_data["wind_meridional"] ** 2
    ) ** 0.5
    weather_data = weather_data.round({"latitude": 2, "longitude": 2})
    # columns ghi ist accumulated over 24 hours, so use difference to get hourly values
    # first we need to order by time and location and then calculate the difference
    weather_data = weather_data.sort_values(by=["latitude", "longitude", "time"])
    weather_data["ghi"] = weather_data["ghi"].diff()
    # set negatives to 0
    weather_data["ghi"] = weather_data["ghi"].clip(lower=0)
    # nan to 0
    weather_data["ghi"] = weather_data["ghi"].fillna(0)
    # set ghi at 00:00 to 0
    weather_data.loc[weather_data["time"].dt.hour == 0, "ghi"] = 0
    weather_data = weather_data.set_index(["time", "latitude", "longitude"])

    log.info("preparing to write dataframe into ecmwf database")
    # write to database
    if write_lat_lon:
        try:
            weather_data.to_sql(
                "ecmwf",
                con=engine,
                if_exists="append",
                chunksize=10000,
                method=psql_insert_copy,
            )
        except Exception:
            log.error(
                "no postgresql? - could not write using psql_insert_copy - using multi method"
            )
            weather_data.to_sql(
                "ecmwf", con=engine, if_exists="append", chunksize=10000
            )

    nuts3 = gpd.read_file(NUTS_PATH)
    # use only nuts_id and coordinates from nuts file so fewer columns have to be joined
    nuts3 = nuts3.loc[:, ["NUTS_ID", "geometry"]]
    nuts3 = nuts3.set_index("NUTS_ID")
    weather_data = weather_data.reset_index()
    weather_data["coords"] = list(
        zip(weather_data["longitude"], weather_data["latitude"])
    )
    weather_data["coords"] = weather_data["coords"].apply(Point)
    weather_data = gpd.GeoDataFrame(weather_data, geometry="coords", crs=nuts3.crs)
    # join weather data to nuts areas
    weather_data = gpd.sjoin(weather_data, nuts3, predicate="within", how="left")
    weather_data = pd.DataFrame(weather_data)
    # coords columns only necessary for the join
    weather_data = weather_data.drop(columns="coords")
    weather_data = weather_data.rename(columns={"NUTS_ID": "nuts_id"})
    weather_data = weather_data.dropna(axis=0)
    # calculate average for all locations inside the current nuts area
    weather_data = weather_data.groupby(["time", "nuts_id"]).mean(numeric_only=True)
    weather_data = weather_data.reset_index()
    weather_data = weather_data.set_index(["time", "latitude", "longitude", "nuts_id"])
    log.info(
        "preparing to write nuts dataframe for %s into ecmwf_eu database", filename
    )
    try:
        weather_data.to_sql(
            "ecmwf_eu",
            con=engine,
            if_exists="append",
            chunksize=10000,
            method=psql_insert_copy,
        )
    except Exception:
        log.error(
            "no postgresql? - could not write using psql_insert_copy - using multi method"
        )
        weather_data.to_sql("ecmwf_eu", con=engine, if_exists="append", chunksize=10000)

    # Delete files locally to save space
    for file in Path(file_path.parent).rglob(file_path.name + "*"):
        try:
            if file.is_dir():
                shutil.rmtree(file)
            else:
                file.unlink()
            log.info("removed file %s", file)
        except OSError as e:
            log.error(f"Error removing files: {e}")


def daterange(start_date: datetime, end_date: datetime = None):
    if not end_date:
        end_date = date.today()
    for n in range(
        int((datetime.combine(end_date, datetime.min.time()) - start_date).days)
    ):
        yield start_date + timedelta(days=n)


def request_list_from_dates(dates: list[datetime]) -> list[dict]:
    dates_dataframe = pd.DataFrame(dates, columns=["Date"])
    grouped_by_months = dates_dataframe.groupby(pd.Grouper(key="Date", freq="ME"))
    months = [group for _, group in grouped_by_months]

    requests_list = []
    for month in months:
        days = []
        for i in range(month.index.start, month.index.stop):
            days.append(f"{month['Date'].dt.day[i]:02d}")
        day_chunks = divide_month_in_chunks(days, 8)
        for chunk in day_chunks:
            request = dict(
                format="grib",
                variable=weather_variables,
                year=f"{month['Date'].dt.year[month.index.start]}",
                month=f"{month['Date'].dt.month[month.index.start]:02d}",
                day=chunk,
                time=[f"{i:02d}:00" for i in range(24)],
            )
            request["area"] = coords

            requests_list.append(request)
    return requests_list


def single_day_request(last_date: datetime):
    request = dict(
        format="grib",
        variable=weather_variables,
        year=f"{last_date.year}",
        month=f"{last_date.month:02d}",
        day=f"{last_date.day:02d}",
        time=[f"{i:02d}:00" for i in range(last_date.hour, 24)],
    )
    request["area"] = coords
    return request


def divide_month_in_chunks(li, n):
    ch = []
    for i in range(0, len(li), n):
        ch.append(li[i : i + n])
    return ch


class EcmwfCrawler(ContinuousCrawler):
    # Data is only available after 6 hours - so we should not crawl to far
    # Otherwise we only receive null values
    OFFSET_FROM_NOW = timedelta(days=2)

    def __init__(self, schema_name: str, config: CrawlerConfig):
        super().__init__(schema_name, config)
        self.ecmwf_client = cdsapi.Client()
        TEMP_DIR.mkdir(parents=True, exist_ok=True)

    def get_latest_data(self) -> datetime:
        query = text("select max(time) from ecmwf")
        try:
            with self.engine.connect() as conn:
                return conn.execute(query).scalar() or TEMPORAL_START
        except Exception:
            log.error("No ecmwf latest found")
            return TEMPORAL_START

    def get_first_data(self) -> datetime:
        query = text("select min(time) from ecmwf")
        try:
            with self.engine.connect() as conn:
                return conn.execute(query).scalar() or TEMPORAL_START
        except Exception:
            log.error("No ecmwf first found")
            return TEMPORAL_START

    def create_hypertable_if_not_exists(self) -> None:
        self.create_single_hypertable_if_not_exists("ecmwf", "time")
        self.create_single_hypertable_if_not_exists("ecmwf_eu", "time")

    def crawl_from_to(self, begin: datetime, end: datetime):
        """Crawls data from begin (inclusive) until end (exclusive)

        Args:
            begin (datetime): included begin datetime from which to crawl
            end (datetime): exclusive end datetime until which to crawl
        """
        if begin < TEMPORAL_START:
            begin = TEMPORAL_START

        data_available_until = datetime.now() - self.get_minimum_offset()

        if end > data_available_until:
            end = data_available_until

        last_date = self.get_latest_data()
        last_date += timedelta(hours=1)
        if last_date > begin:
            begin = last_date

        # the requests are build from 00:00 - 23:00 for each day
        # however, for recent dates the cds API delivers data up until the latest hour of the day it can deliver
        # that is why a check is necessary to first make sure that the database has dates up until 23:00

        if begin.hour != 23:
            log.info("Creating request for single day")
            request = single_day_request(begin)
            log.info(f"The current request running: {request}")
            save_ecmwf_request_to_file(request, self.ecmwf_client)
            build_dataframe(self.engine, request)
            begin = self.get_latest_data()

        dates = []
        for single_date in daterange(begin, end):
            dates.append(single_date)

        for request in request_list_from_dates(dates):
            log.info(f"The current request running: {request}")
            save_ecmwf_request_to_file(request, self.ecmwf_client)
            build_dataframe(self.engine, request)


if __name__ == "__main__":
    logging.basicConfig(filename="ecmwf.log", encoding="utf-8", level=logging.INFO)
    from pathlib import Path

    config = load_config(DEFAULT_CONFIG_LOCATION)
    smard = EcmwfCrawler("ecmwf", config=config)
    smard.crawl_temporal()
