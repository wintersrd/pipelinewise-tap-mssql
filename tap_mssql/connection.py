#!/usr/bin/env python3

import backoff
import pymssql
import singer
from os import environ

LOGGER = singer.get_logger()


@backoff.on_exception(backoff.expo, pymssql.Error, max_tries=5, factor=2)
def connect_with_backoff(connection):
    warnings = []
    with connection.cursor():
        if warnings:
            LOGGER.info(
                (
                    "Encountered non-fatal errors when configuring session that could "
                    "impact performance:"
                )
            )
        for w in warnings:
            LOGGER.warning(w)

    return connection


class MSSQLConnection(pymssql.Connection):
    def __init__(self, config):
        args = {
            "user": config.get("user"),
            "password": config.get("password"),
            "server": config["host"],
            "database": config["database"],
            "charset": config.get("characterset", "utf8"),
            "port": config.get("port", "1433"),
            "tds_version": config.get("tds_version", "7.3"),
        }
        # Add additional conn_properties for specific version settings
        if config.get("conn_properties"):
            args["conn_properties"] = config.get("conn_properties")
        # Optional ability to dump TDS logs
        if config.get("enable_tds_logging"):
            environ['TDSDUMP'] = 'stderr'
        conn = pymssql._mssql.connect(**args)
        super().__init__(conn, False, True)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        del exc_info
        self.close()


def make_connection_wrapper(config):
    class ConnectionWrapper(MSSQLConnection):
        def __init__(self, *args, **kwargs):
            super().__init__(config)

            connect_with_backoff(self)

    return ConnectionWrapper

def ResultIterator(cursor, arraysize=1):
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result
