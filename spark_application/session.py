"""
Module to keep a shared spark session
"""

import logging

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def session():
    """
    :return: A spark session object
    """
    return SparkSession.builder.getOrCreate()
