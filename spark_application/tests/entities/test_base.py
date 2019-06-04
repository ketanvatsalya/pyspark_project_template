import unittest
from spark_application.entities.base import Entity
from spark_application import session

from pyspark.sql.types import StringType, IntegerType, StructType


class MyEntity(Entity):

    @property
    def schema(self):
        schema = StructType()
        schema.add('Name', StringType(), True)
        schema.add('Age', IntegerType(), True)
        return schema

    def get_data(self, data):
        self.data = data


class EntityWithWrongSchemaType(Entity):

    @property
    def schema(self):
        schema = 'Awesome Schema'
        return schema

    def get_data(self, data):
        self.data = data


class TestBaseEntity(unittest.TestCase):

    def setUp(self) -> None:
        self.session = session()

    def tearDown(self) -> None:
        pass

    def test_wrong_schema_type_raises_error(self):
        self.assertRaises(TypeError, EntityWithWrongSchemaType)

    def test_wrong_data_type_raises_error(self):
        data = [['A', 1], ['B', 2]]
        self.assertRaises(AssertionError, MyEntity, data)

    def test_wrong_schema_raises_error(self):
        data = [['A', 1], ['B', 2]]

        schema = StructType()
        schema.add('Name', StringType(), True)
        schema.add('Group', IntegerType(), True)

        data_df = self.session.createDataFrame(data, schema=schema)

        self.assertRaises(AssertionError, MyEntity, data_df)
