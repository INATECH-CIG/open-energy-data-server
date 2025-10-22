# SPDX-FileCopyrightText: Marvin Lorber
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
Crawler for data from https://www.netztransparenz.de.
The resulting data is not available under an open-source license and should not be reshared but is available for crawling yourself.

Using this crawler requires setting up an Account and client in the
Netztransparenz extranet (see: https://www.netztransparenz.de/de-de/Web-API).

The client expects that the environment contains the variables 'IPNT_CLIENT_ID' and 'IPNT_CLIENT_SECRET'
with the credentials from the previous step.
"""

import logging
from datetime import UTC, datetime, timedelta

import netztransparenz as nt
import sqlalchemy as sql

from oeds.base_crawler import (
    DEFAULT_CONFIG_LOCATION,
    ContinuousCrawler,
    CrawlerConfig,
    load_config,
)

log = logging.getLogger("netztransparenz")
log.setLevel(logging.INFO)
api_date_format = "%Y-%m-%dT%H:%M:%S"
csv_date_format = "%Y-%m-%d %H:%M %Z"

metadata_info = {
    "schema_name": "netztransparenz",
    "data_date": "2015-02-10",
    "data_source": "https://ds.netztransparenz.de/api/",
    "license": "https://www.netztransparenz.de/en/About-us/Information-platforms/Disclosure-obligations-in-accordance-with-the-EU-Transparency-Regulation",
    "description": "German Energy Network Operations. Activated minimum and secondary reserve levels, forecasted solar and wind energy outputs, net reserve power balance, and redispatch measures.",
    "contact": "",
    "temporal_start": "2011-03-31 00:00:00",
    "temporal_end": "2024-05-22 16:00:00",
    "concave_hull_geometry": None,
}


def database_friendly(string):
    return string.lower().replace("(", "").replace(")", "").replace(" ", "_")


TEMPORAL_START = datetime(2011, 3, 31)


class NetztransparenzCrawler(ContinuousCrawler):
    TIMEDELTA = timedelta(days=2)

    def __init__(self, schema_name: str, config: CrawlerConfig):
        super().__init__(schema_name, config)
        self.initialize_token()

    def get_latest_data(self) -> datetime:
        return TEMPORAL_START

    def get_first_data(self) -> datetime:
        return TEMPORAL_START

    def initialize_token(self):
        # add your Client-ID and Client-secret from the API Client configuration GUI to
        # your environment variable first

        IPNT_CLIENT_ID = self.config.get("ipnt_client_id")
        IPNT_CLIENT_SECRET = self.config.get("ipnt_client_secret")
        self.client = nt.NetztransparenzClient(
            IPNT_CLIENT_ID, IPNT_CLIENT_SECRET, strict=False
        )

    def check_health(self):
        print(self.client.check_health())

    def forecast_solar(self):
        df = self.client.prognose_solar(transform_dates=True)
        with self.engine.begin() as conn:
            df.to_sql("prognose_solar", conn, if_exists="replace")

    def forecast_wind(self):
        df = self.client.prognose_wind(transform_dates=True)
        with self.engine.begin() as conn:
            df.to_sql("prognose_wind", conn, if_exists="replace")

    def extrapolation_solar(
        self, begin: datetime | None = None, end: datetime | None = None
    ):
        df = self.client.hochrechnung_solar(begin, end, True)
        df.rename(
            mapper=lambda x: database_friendly(x),
            axis="columns",
            inplace=True,
        )
        with self.engine.begin() as conn:
            df.to_sql("hochrechnung_solar", conn, if_exists="append")

    def extrapolation_wind(
        self, begin: datetime | None = None, end: datetime | None = None
    ):
        df = self.client.hochrechnung_wind(begin, end, True)
        df.rename(
            mapper=lambda x: database_friendly(x),
            axis="columns",
            inplace=True,
        )
        with self.engine.begin() as conn:
            df.to_sql("hochrechnung_wind", conn, if_exists="append")

    def utilization_balancing_energy(
        self, begin: datetime | None = None, end: datetime | None = None
    ):
        df = self.client.vermarktung_inanspruchnahme_ausgleichsenergie(begin, end, True)
        df.rename(
            mapper=lambda x: database_friendly(x),
            axis="columns",
            inplace=True,
        )
        with self.engine.begin() as conn:
            df.to_sql(
                "vermarktung_inanspruchnahme_ausgleichsenergie",
                conn,
                if_exists="append",
            )

    def redispatch(self, begin: datetime | None = None, end: datetime | None = None):
        df = self.client.redispatch(begin, end, True)
        df.rename(mapper=str.lower, axis="columns", inplace=True)

        with self.engine.begin() as conn:
            df.to_sql("redispatch", conn, if_exists="append")

    def gcc_balance(self, begin: datetime, end: datetime):
        df = self.client.nrvsaldo_nrvsaldo_qualitaetsgesichert(begin, end, True)
        df.rename(mapper=str.lower, axis="columns", inplace=True)
        with self.engine.begin() as conn:
            df.to_sql("nrv_saldo", conn, if_exists="append")

    def lfc_area_balance(self, begin: datetime, end: datetime):
        df = self.client.nrvsaldo_rzsaldo_qualitaetsgesichert(begin, end, True)
        df.rename(mapper=str.lower, axis="columns", inplace=True)
        with self.engine.begin() as conn:
            df.to_sql("rz_saldo", conn, if_exists="append")

    def activated_automatic_balancing_capacity(self, begin: datetime, end: datetime):
        df = self.client.nrvsaldo_aktivierte_srl_qualitaetsgesichert(begin, end, True)
        df.rename(mapper=str.lower, axis="columns", inplace=True)
        with self.engine.begin() as conn:
            df.to_sql("aktivierte_srl", conn, if_exists="append")

    def activated_manual_balancing_capacity(self, begin: datetime, end: datetime):
        df = self.client.nrvsaldo_aktivierte_mrl_qualitaetsgesichert(begin, end, True)
        df.rename(mapper=str.lower, axis="columns", inplace=True)
        with self.engine.begin() as conn:
            df.to_sql("aktivierte_mrl", conn, if_exists="append")

    def value_of_avoided_activation(self, begin: datetime, end: datetime):
        df = self.client.nrvsaldo_voaa_qualitaetsgesichert(begin, end, True)
        df.rename(mapper=str.lower, axis="columns", inplace=True)
        with self.engine.begin() as conn:
            df.to_sql("value_of_avoided_activation", conn, if_exists="append")

    def find_latest(self, tablename: str, column_name: str, default):
        try:
            with self.engine.begin() as conn:
                query = sql.text(f"SELECT max({column_name}) FROM {tablename}")
                result = conn.execute(query).scalar() or default
                return result
        except Exception:
            return default

    def create_hypertable_if_not_exists(self) -> None:
        for tablename in [
            "prognose_solar",
            "prognose_wind",
            "hochrechnung_solar",
            "hochrechnung_wind",
            "vermarktung_inanspruchnahme_ausgleichsenergie",
            "redispatch",
            "nrv_saldo",
            "rz_saldo",
            "aktivierte_srl",
            "aktivierte_mrl",
            "value_of_avoided_activation",
        ]:
            self.create_single_hypertable_if_not_exists(tablename, "von")

    def check_table_exists(self, tablename):
        return sql.inspect(self.engine).has_table(tablename)

    def crawl_temporal(
        self, begin: datetime | None = None, end: datetime | None = None
    ):
        # crawler.check_health()
        if not self.check_table_exists("prognose_solar"):
            log.info("No Solar")
            self.forecast_solar()
        if not self.check_table_exists("prognose_wind"):
            log.info("No Wind")
            self.forecast_wind()

        temporal_funcs = [
            # 0=function, 1=start of data, 2=crawling delay, 3=table name, 4=index name in table
            (
                self.extrapolation_wind,
                datetime(2011, 3, 31, 22, 0),
                timedelta(days=1),
                "hochrechnung_wind",
                "von",
            ),
            (
                self.extrapolation_solar,
                datetime(2011, 3, 31, 22, 0),
                timedelta(days=1),
                "hochrechnung_solar",
                "von",
            ),
            (
                self.utilization_balancing_energy,
                datetime(2011, 3, 31, 22, 0),
                timedelta(days=1),
                "vermarktung_inanspruchnahme_ausgleichsenergie",
                "von",
            ),
            (
                self.redispatch,
                datetime(2021, 10, 1, 0, 0),
                timedelta(days=1),
                "redispatch",
                "beginn",
            ),
            (
                self.activated_automatic_balancing_capacity,
                datetime(2014, 2, 25, 23, 0),
                timedelta(days=1),
                "aktivierte_srl",
                "von",
            ),
            (
                self.activated_manual_balancing_capacity,
                datetime(2011, 6, 26, 22, 0),
                timedelta(days=1),
                "aktivierte_mrl",
                "von",
            ),
            (
                self.value_of_avoided_activation,
                datetime(2023, 11, 1, 0, 0),
                timedelta(days=1),
                "value_of_avoided_activation",
                "von",
            ),
            (
                self.gcc_balance,
                datetime(2023, 11, 1, 0, 0),
                timedelta(days=30),
                "nrv_saldo",
                "von",
            ),
            (
                self.lfc_area_balance,
                datetime(2023, 11, 1, 0, 0),
                timedelta(days=30),
                "rz_saldo",
                "von",
            ),
        ]
        for func_tuple in temporal_funcs:
            log.info(f"Processing Table {func_tuple[3]}")
            latest = self.find_latest(func_tuple[3], func_tuple[4], func_tuple[1])
            current_begin = begin
            current_end = end
            if current_begin is None or (
                current_begin.astimezone(UTC) < latest.astimezone(UTC)
                ):
                current_begin = latest.astimezone(UTC)
            earliest = datetime.now(tz=UTC) - func_tuple[2]
            if current_end is None or current_end > earliest:
                current_end = earliest
            log.info(f"Crawling from {current_begin} to {current_end}")
            func_tuple[0](current_begin, current_end)


if __name__ == "__main__":
    logging.basicConfig()

    config = load_config(DEFAULT_CONFIG_LOCATION)
    crawl = NetztransparenzCrawler("netztransparenz", config=config)
    crawl.crawl_temporal()
    crawl.set_metadata(metadata_info)
