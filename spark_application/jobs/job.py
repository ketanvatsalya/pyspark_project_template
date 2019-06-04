import abc
from pyspark import SparkConf


class Job(object):
    """
    Base class to define the template of a pyspark job. The template hopes to achieve idempotence with as less
    boiler plate code as possible
    """

    def __init__(self):

        self.results_df = None

        if not isinstance(self.conf, SparkConf):
            raise TypeError('conf property must be of type : {type}'.format(type=repr(SparkConf)))

    @property
    @abc.abstractmethod
    def conf(self):
        """
        This property should define the spark configs to be used when submitting the job
        :return: conf (SparkConf)
        """
        pass

    @abc.abstractmethod
    def definition(self):
        """
        The workhorse method of every job class; defines the spark transformations to be applied
        ideally this method should just daisy chain transformations with as little additional code
        as possible
        :return: results_df, A Spark Dataframe
        """
        pass

    def persist(self, datastore, *args, **kwargs):
        """
        Writes the results back to a datastore
        :return: None
        """

        try:
            datastore.write(self.results_df, *args, **kwargs)
        except Exception:
            raise

    def clean_up(self):
        """
        Ideally the job definition should not create any side effects and results should be written back
        only through the publish method. However if such side-effects exist this method can be overridden
        to clean them up in the event of a job failure.
        :return:
        """
        pass
