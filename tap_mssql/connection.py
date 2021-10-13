#!/usr/bin/env python3

import backoff

import pyodbc

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

import singer
import ssl

LOGGER = singer.get_logger()


@backoff.on_exception(backoff.expo, pyodbc.Error, max_tries=5, factor=2)
def connect_with_backoff(connection):
    warnings = []
    with connection.cursor() as cur:
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


def get_azure_sql_engine(config) -> Engine:
    """The All-Purpose SQL connection object for the Azure Data Warehouse."""

    conn_values = {
        "prefix": "mssql+pyodbc://",
        "username": config["user"],
        "password": config["password"],
        "port": config.get("port", "1433"),
        "host": config["host"],
        "driver": "ODBC+Driver+17+for+SQL+Server",
        "database": config["database"],
    }

    conn_values["authentication"] = "SqlPassword"
    raw_conn_string = "{prefix}{username}:{password}@{host}:\
{port}/{database}?driver={driver}&Authentication={authentication}&\
autocommit=True&IntegratedSecurity=False"

    engine = create_engine(raw_conn_string.format(**conn_values))
    return engine
