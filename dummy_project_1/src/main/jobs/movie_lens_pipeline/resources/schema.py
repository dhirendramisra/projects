from pyspark.sql.types import StructType, StringType, StructField, IntegerType, LongType

# ratings.dat file
# Columns (UserID::MovieID::Rating::Timestamp)
# Data Sample (1::1193::5::978300760)
ratings = StructType([
    StructField("UserID", IntegerType()),
    StructField("MovieID", IntegerType()),
    StructField("Rating", IntegerType()),
    StructField("Timestamp", LongType())
])

# users.dat file
# Columns (UserID::Gender::Age::Occupation::Zip-code)
# Data Sample (1::F::1::10::48067)
users = StructType([
    StructField("UserID", IntegerType()),
    StructField("Gender", StringType()),
    StructField("Age", IntegerType()),
    StructField("Occupation", IntegerType()),
    StructField("Zip-code", StringType())
])

# movies.dat file
# Columns (MovieID::Title::Genres) 
# Data Sample (1::Toy Story (1995)::Animation|Children's|Comedy)
movies = StructType([
    StructField("MovieID", IntegerType()),
    StructField("Title", StringType()),
    StructField("Genres", StringType())
])