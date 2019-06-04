"""
This package holds all the job definitions.
Every job is an idempotent process comprising of one or more transformations.
It returns results as a Spark dataframe. The result can be written back to a datastore.
Every job should return only one results dataframe.

A job should also define the spark configs it deems appropriate for it's efficient execution.
"""

from .job import Job
