import abc
from spark_application import session
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame


class Transformation(object):
    """
    Base Transformation class to serve as a template for all transformations

    """
    __input_schema = None
    __output_schema = None

    def __init__(self):
        self.session = session()

        if not isinstance(self.input_schema, StructType):
            raise TypeError('Input schema must be of {type} type'.format(type=repr(StructType())))

        if not isinstance(self.output_schema, StructType):
            raise TypeError('Output schema must be of {type} type'.format(type=repr(StructType())))

    def transform(self, input_df, *args, **kwargs):
        """
        Applies the class transformation to input_df and returns the output_df
        :param input_df : (pyspark.sql.dataframe.DataFrame): input dataframe; must match the input_schema
        :return: output_df (pyspark.sql.dataframe.DataFrame) : output dataframe; must match the output_schema
        """

        if not isinstance(input_df, DataFrame):
            raise TypeError(
                'input_df must be of the type : {type}'.format(type=repr(DataFrame)))

        if not input_df.schema == self.input_schema:
            raise ValueError(
                'input_df does not match the required schema : {schema}'.format(schema=self.input_schema.json()))

        # Apply the transformation to input_df
        output_df = self.transformation(input_df, *args, **kwargs)

        if not output_df.schema == self.output_schema:
            raise ValueError(
                'output df does not match the required schema : {schema}'.format(schema=self.output_schema.json()))

        return output_df

    @property
    @abc.abstractmethod
    def input_schema(self):
        """
        Abstract property; specifies the schema expected of the input_df
        """
        pass

    @property
    @abc.abstractmethod
    def output_schema(self):
        """
        Abstract property; specifies the schema expected of the output_df
        """
        pass

    @abc.abstractmethod
    def transformation(self, input_df, *args, **kwargs):
        """
        Each Transformation class must define a transformation method to transform the input_df into the output_df
        :param input_df : (pyspark.sql.dataframe.DataFrame): input dataframe; must match the input_schema
        :return: output_df (pyspark.sql.dataframe.DataFrame) : output dataframe; must match the output_schema
        """
        pass
