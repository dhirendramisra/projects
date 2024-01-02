import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, LongType, DoubleType
from pyspark_test import assert_pyspark_df_equal

from constant import key
import main.jobs.movie_lens_pipeline.resources.schema as schema
from main.jobs.movie_lens_pipeline.movie_lens_pipeline import PySparkJobMovieLens

job_name = "movie_lens_pipeline"

job = PySparkJobMovieLens(job_name)

# MovieID::Title::Genres
movie_sample = [
    (1, "Toy Story (1995)", "Animation|Children's|Comedy"),
    (2, "Jumanji (1995)", "Adventure|Children's|Fantasy"),
    (3, "Grumpier Old Men (1995)", "Comedy|Romance"),
    (4, "Waiting to Exhale (1995)", "Comedy|Drama"),
    (5, "Father of the Bride Part II (1995)", "Comedy")
]

# UserID::Gender::Age::Occupation::Zip-code
user_sample = [
    (1,"F",18,10,"48067"), (2,"M",18,10,"70072")
]

# UserID::MovieID::Rating::Timestamp
ratings_sample = [
    (1,1,2,978300760),
    (1,2,4,978300760),
    (1,3,3,978300760),
    (2,1,5,978300760),
    (2,2,5,978300760),
    (2,3,5,978300760),
    (3,3,4,978300760),
    (3,2,3,978300760),
    (3,4,1,978300760)
]

# For movie statistics 
expected_stats = [
    (1, "Toy Story (1995)", "Animation|Children's|Comedy", 2, 5, 3.5),
    (2, "Jumanji (1995)", "Adventure|Children's|Fantasy", 3, 5, 4.0),
    (3, "Grumpier Old Men (1995)", "Comedy|Romance", 3, 5, 4.0),
    (4, "Waiting to Exhale (1995)", "Comedy|Drama", 1, 1, 1.0),
    (5, "Father of the Bride Part II (1995)", "Comedy", None, None, None)
]

schema_expected_stats = StructType([
    StructField("MovieID", IntegerType()),
    StructField("Title", StringType()),
    StructField("Genres", StringType()),
    StructField("MinRating", IntegerType()),
    StructField("MaxRating", IntegerType()),
    StructField("AvgRating", DoubleType())
])

# For any top 3 movies of each user based on user's ratings for the movies
expected_user_top_movies = [
    (1, 2, "Jumanji (1995)", 4, 1),
    (1, 3, "Grumpier Old Men (1995)", 3, 2),
    (1, 1, "Toy Story (1995)", 2, 3),
    (2, 1, "Toy Story (1995)", 5, 1),
    (2, 2, "Jumanji (1995)", 5, 2),
    (2, 3, "Grumpier Old Men (1995)", 5, 3),
    (3, 3, "Grumpier Old Men (1995)", 4, 1),
    (3, 2, "Jumanji (1995)", 3, 2),
    (3, 4, "Waiting to Exhale (1995)", 1, 3)
]

schema_expected_top_movies = StructType([
    StructField("UserID", IntegerType()),
    StructField("MovieID", IntegerType()),
    StructField("Title", StringType()),
    StructField("Rating", IntegerType()),
    StructField("RatingNumber", IntegerType())
])

def create_sample(sample, data_schema):
    return job.spark.createDataFrame(data=sample, schema=data_schema)


@pytest.mark.filterwarnings("ignore")
def test_init_spark_session():
    assert isinstance(job.spark, SparkSession), "-- spark session not implemented"    


@pytest.mark.filterwarnings("ignore")
def test_get_movie_rating_stats():
    df_movies = create_sample(movie_sample, schema.movies)
    df_rating = create_sample(ratings_sample, schema.ratings)
    df_expected_stats = create_sample(expected_stats, schema_expected_stats) 

    df_transformed = job.get_movie_rating_stats_sql(df_movies, df_rating)
    #df_transformed = job.get_movie_rating_stats(df_movies, df_rating)
    cnt = df_transformed.count()
    assert (5 == cnt)
    assert_pyspark_df_equal(df_transformed,  df_expected_stats)


@pytest.mark.filterwarnings("ignore")
def test_get_each_user_top_movies():
    df_movies = create_sample(movie_sample, schema.movies)
    df_rating = create_sample(ratings_sample, schema.ratings)
    df_expected_user_top_movies = create_sample(expected_user_top_movies, schema_expected_top_movies) 

    df_transformed = job.get_each_user_top_movies_sql(df_movies, df_rating)
    #df_transformed = job.get_each_user_top_movies(df_movies, df_rating)
    #df_transformed.show()
    assert_pyspark_df_equal(df_transformed,  df_expected_user_top_movies)

