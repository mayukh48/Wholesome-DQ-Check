"""Shared pytest fixtures for the test suite.

A single SparkSession is reused across all tests (session scope) because
starting a SparkSession is slow - creating a new one per test would make the
suite take much longer for no benefit.
"""
import os

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    # Inside a Databricks notebook/job - classic or serverless - a Spark
    # session is already running (serverless compute only speaks Spark
    # Connect via SPARK_REMOTE and outright rejects a second, local-mode
    # session), so reuse it instead of starting our own.
    if "DATABRICKS_RUNTIME_VERSION" in os.environ or "SPARK_REMOTE" in os.environ:
        yield SparkSession.builder.getOrCreate()
        return

    session = (
        SparkSession.builder.master("local[2]")
        .appName("pyspark-dq-tests")
        # Shuffle partitions default to 200, which is wasteful overhead for
        # the tiny in-memory test DataFrames used here.
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()
