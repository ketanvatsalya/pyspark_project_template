from spark_application.entities.base import Entity
from pyspark.sql.types import StructType, StringType, IntegerType

import logging
import pkg_resources

logger = logging.getLogger(__name__)


class Employees(Entity):

    @property
    def schema(self):
        schema = StructType()
        schema.add('Name', StringType(), True)
        schema.add('Department', StringType(), True)
        schema.add('Manager', StringType(), True)
        schema.add('Salary', IntegerType(), True)

        return schema

    def get_data(self):
        path_to_the_csv_file = pkg_resources.resource_filename('spark_application.data', 'employees.csv')
        df = self.session.read.format("csv").option("header", "true").load(path_to_the_csv_file)

        for field in df.schema.fields:
            df = df.withColumn(field.name, df[field.name].cast(self.schema[field.name].dataType))

        self.data = df
