"""
base entities module
"""

from abc import abstractmethod
from spark_application import session
from pyspark.sql.dataframe import DataFrame


class Entity(object):
    """
    Base entities class; srves as a template for other entity classes
    
    """

    __schema = None
    __data = None

    def __init__(self, *args, **kwargs):
        self.session = session()
        self.get_data(*args, **kwargs)

    @property
    @abstractmethod
    def schema(self):
        return self.__schema

    @schema.setter
    def schema(self, value):
        # TODO assert schema has valid type
        self.__schema = value

    @property
    def data(self):
        return self.__data

    @data.setter
    def data(self, value):

        # Checking is value supplied is a dataframe
        if not isinstance(value, DataFrame):
            raise AssertionError('data property must be a spark dataframe')

        # Checking if value supplied matches the required schema
        if not (value.schema == self.schema):
            raise AssertionError('Dataframe schema does not match the expected schema')

        self.__data = value

    @abstractmethod
    def get_data(self, *args, **kwargs):
        """

        :return: assign a spark dataframe matching the schema to self.data
        """
        pass
