from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

"""
This class is for common data validation. As a sample one method has been implemented.
"""
class DataValidation():

    def __init__(self) :
        pass

    @classmethod
    def check_null(cls, df : DataFrame, col_name):
        # Counts number of nulls and nans in each column   
        df_target = df.filter(df[col_name].isNull())
        cnt = df_target.count()
        return cnt