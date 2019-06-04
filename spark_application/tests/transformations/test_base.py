import unittest
from spark_application.transformations.base import Transformation
from spark_application import session

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, StructType


class MyTransformation(Transformation):

    @property
    def input_schema(self):
        schema = StructType()
        schema.add('Batsman', StringType(), True)
        schema.add('Match', StringType(), True)
        schema.add('Runs', IntegerType(), True)
        return schema

    @property
    def output_schema(self):
        schema = StructType()
        schema.add('Match', StringType(), True)
        schema.add('Runs', IntegerType(), True)
        return schema

    def transformation(self, input_df):
        # Calculate total runs per match
        return input_df.groupBy('Match').agg(F.sum(input_df['Runs']))


class TransformationWithWrongInputSchemaType(Transformation):

    @property
    def input_schema(self):
        schema = 'AwesomeSchema'
        return schema

    @property
    def output_schema(self):
        schema = StructType()
        return schema

    def transformation(self, input_df):
        # Calculate total runs per match
        return input_df


class TransformationWithWrongOutputSchemaType(Transformation):

    @property
    def input_schema(self):
        schema = StructType()
        return schema

    @property
    def output_schema(self):
        schema = 'AwesomeSchema'
        return schema

    def transformation(self, input_df):
        # Calculate total runs per match
        return input_df


class TestBaseTransformation(unittest.TestCase):

    def setUp(self) -> None:
        self.session = session()

    def tearDown(self) -> None:
        pass

    def test_wrong_input_schema_type_raises_error(self):
        self.assertRaises(TypeError, TransformationWithWrongInputSchemaType)

    def test_wrong_output_schema_type_raises_error(self):
        self.assertRaises(TypeError, TransformationWithWrongOutputSchemaType)

    def test_wrong_input_type_raises_error(self):
        input_df = 'WrongType'
        trans = MyTransformation()
        self.assertRaises(TypeError, trans.transform, input_df)

    def test_wrong_input_schema_raises_error(self):
        input_schema = StructType().add('Batsman', StringType(), True)
        data = [['Sachin'], ['Dravid'], ['Laxman']]
        input_df = self.session.createDataFrame(data, schema=input_schema)

        trans = MyTransformation()
        self.assertRaises(ValueError, trans.transform, input_df)

    def test_wrong_output_schema_raises_error(self):
        input_schema = StructType()
        input_schema.add('Batsman', StringType(), True)
        input_schema.add('Match', StringType(), True)
        input_schema.add('Runs', IntegerType(), True)
        data = [['Sachin', 'M1', 100], ['Dravid', 'M1', 50], ['Laxman', 'M2', 150]]
        input_df = self.session.createDataFrame(data, schema=input_schema)

        trans = MyTransformation()
        trans.output_schema.add('Batsman', StringType(), True)
        self.assertRaises(ValueError, trans.transform, input_df)
