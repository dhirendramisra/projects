import logging
import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame, Window

from constant import key
from main.base import PySparkJobInterface
import main.jobs.baby_name_pipeline.resources.schema as schema

# Activate logging
logger = logging.getLogger(__name__)
logger.info("Inside baby_name pipeline")


""" 
This is the child class which inherits base class PySparkJobInterface. This class
implements all functionalities which are required for the baby_name job.  
"""  
class PySparkJobBabyName(PySparkJobInterface):

    def __init__(self, app_name) :
        self.spark = self.init_spark_session(app_name)

    def init_spark_session(self, app_name) -> SparkSession :
        try:
            if key.ENV.value == key.PRODUCTION.value :
                master = key.MASTER_YARN.value
            else :
                master = key.MASTER_LOCAL.value

            spark = SparkSession.builder.master(master).appName(app_name).getOrCreate()
        except Exception as exp:
            logger.error(str(exp))
            raise
        return spark
        
    # Read Baby name datafrom csv file
    # def read_csv(self, input_path: str, schema: StructType = None, header: bool = False, separater:str = ',') -> DataFrame:
    def read_baby_name_data(self, job_config) -> DataFrame:
        try:
            logger.info("Reading baby name data..")
            df = self.read_csv(job_config["bname_input_path"], schema.baby_name, job_config["header"])
        except Exception as exp:
            logger.error(str(exp))
            raise
        return df



"""run() method to execute the job pipeline"""
def run(job_config):
    try:
        # Instantiate baby name
        job = PySparkJobBabyName(job_config["job_name"])
        logger.info("Start of Job {} has started.".format(job_config["job_name"]))

        logger.info("Read baby name data into dataframe")
        df_bname   = job.read_baby_name_data(job_config)
        logger.info("Count of baby name dataset {}.".format(df_bname.count()))

        logger.info("End of Job {}.".format(job_config["job_name"]))
    except Exception as exp :
        raise
    finally :
        # kill the spark object
        job.stop()