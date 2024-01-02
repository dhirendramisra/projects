import abc
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession, DataFrame

""" 
This is the base class which contains common methods, common varibales for 
child classes. 
"""
class PySparkJobInterface(abc.ABC):

    def __init__(self) :
        pass

    @abc.abstractmethod
    def init_spark_session(self, appName: str = "default") -> SparkSession:
        """Create spark session"""
        raise NotImplementedError    


    # Read data from .csv, .dat file formats
    def read_csv(self, input_path: str, schema: StructType = None, header: bool = False, separater:str = ',') -> DataFrame:
        reader = self.spark.read
        if schema is None:
            return reader.options(header=header, inferSchema=True, delimiter=separater).csv(input_path)
        else:
            return reader.options(header=header, delimiter=separater).schema(schema).csv(input_path)        


    # Read data from parquet file format
    def read_parquet(self, input_path: str, schema: StructType = None) -> DataFrame:
        reader = self.spark.read
        # Implement to read from parquet file
        pass


    # Read data from orc file formats
    def read_orc(self, input_path: str, schema: StructType = None) -> DataFrame:
        reader = self.spark.read
        # Implement to read from orc files
        pass

    # Write data in CSV file formats
    def write_csv(self, df_source: DataFrame, file_path: str, header, file_mode: str = "overwrite"):
        df_source.coalesce(1).write.option("header", header).mode(file_mode).format("csv").save(file_path)

    # Write data in ORC file formats
    def write_orc(self, df_source: DataFrame, file_path: str, file_mode: str = "overwrite"):
        df_source.write.mode(file_mode).orc(file_path)

    # Write data in Parquet file formats
    def write_parquet(self, df_source: DataFrame, file_path: str, partition_by_cols: list = [], file_mode: str = "overwrite"):
        if len(partition_by_cols) == 0 :
            df_source.write.mode(file_mode).parquet(file_path)
        else :
            col_list = ""
            for val in partition_by_cols :
                col_list = "'" +  val + "',"
            col_list = col_list.rstrip(",")
            df_source.write.partitionBy(col_list).mode(file_mode).parquet(file_path)   

    # Implement authencation related stuff
    def authenticate() :
        """Can be implemented at pipeline/job level"""
        pass


    # Stop the spark session
    def stop(self) -> None:
        self.spark.stop()