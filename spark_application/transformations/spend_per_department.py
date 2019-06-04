from .base import Transformation
from pyspark.sql.types import StructType, StringType, LongType
from ..entities import employees
import pyspark.sql.functions as f


class SpendPerDepartment(Transformation):

    @property
    def input_schema(self):
        # TODO Employee object should not have to be created to get schema
        schema = employees.Employees().schema
        return schema

    @property
    def output_schema(self):
        schema = StructType()
        schema.add('Department', StringType(), True)
        schema.add('Salary', LongType(), True)
        return schema

    def transformation(self, input_df):
        return input_df.groupBy('Department').agg(f.sum(input_df['Salary']).alias('Salary'))
