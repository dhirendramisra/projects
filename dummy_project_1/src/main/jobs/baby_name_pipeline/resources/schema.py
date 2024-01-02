from pyspark.sql.types import StructType, StringType, StructField, IntegerType, LongType

# baby_name.csv file
# Columns (ID, Name, Country, Timestamp)
# Data Sample (1, Jack, US, 978300760)
baby_name = StructType([
    StructField("ID", IntegerType()),
    StructField("Name", StringType()),
    StructField("Country", StringType()),
    StructField("Timestamp", LongType())
])