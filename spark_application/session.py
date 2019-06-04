"""
Module to keep a shared spark session
"""

import logging

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def session(app_name='PySparkJob'):
    """
    :return: A spark session object
    """
    return SparkSession.builder.appName(app_name).getOrCreate()
