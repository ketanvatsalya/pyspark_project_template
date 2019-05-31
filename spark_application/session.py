"""
Module to keep a shared spark session
"""

import findspark

findspark.init()

from pyspark.sql import SparkSession


def session():
    """
    :return: A spark session object
    """
    return SparkSession.builder.getOrCreate()
