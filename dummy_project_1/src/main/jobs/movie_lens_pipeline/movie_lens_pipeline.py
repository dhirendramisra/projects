import logging
import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame, Window

from constant import key
from main.base import PySparkJobInterface
from main.base.data_validate import DataValidation
import main.jobs.movie_lens_pipeline.resources.schema as schema
import main.jobs.movie_lens_pipeline.sql_model as model

# Activate logging
logger = logging.getLogger(__name__)
logger.info("Inside movie_lens pipeline")


""" 
This is the child class which inherits base class PySparkJobInterface. This class
implements all functionalities which are required for the movie llense job.  
"""  
class PySparkJobMovieLens(PySparkJobInterface):

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
        
    # Read Movie data from dat file
    def read_movie_data(self, job_config) -> DataFrame:
        try:
            logger.info("Reading movie data..")
            df = self.read_csv(job_config["movies_input_path"], schema.movies, job_config["header"], job_config["separater"])
        except Exception as exp:
            logger.error(str(exp))
            raise
        return df

    # Read ratings data from dat file
    def read_rating_data(self, job_config) -> DataFrame:
        try:
            logger.info("Reading rating data..")
            df = self.read_csv(job_config["ratings_input_path"], schema.ratings, job_config["header"], job_config["separater"])
        except Exception as exp:
            logger.error(str(exp))
            raise
        return df

    # Read users data from dat file
    def read_user_data(self, job_config) -> DataFrame:
        try:
            logger.info("Reading user data..")
            df = self.read_csv(job_config["users_input_path"], schema.users, job_config["header"], job_config["separater"])
        except Exception as exp:
            logger.error(str(exp))
            raise
        return df


    # Movie stats -  get all movie data and add ratings's stats if available 
    def get_movie_rating_stats(self, movies: DataFrame, ratings: DataFrame) -> DataFrame:
        try:
            logger.info("Evaluate movie stats and add min, max and avg rating for each movie.")
            df = ratings.withColumn("MinRating", F.min(ratings.Rating).over(Window.partitionBy("MovieID"))).withColumn( 
                    "MaxRating", F.max(ratings.Rating).over(Window.partitionBy("MovieID"))).withColumn( 
                    "AvgRating", F.avg(ratings.Rating).over(Window.partitionBy("MovieID"))).withColumn( 
                    "rn", F.row_number().over(Window.partitionBy("MovieID").orderBy(F.col("UserID"))) 
                    ).filter("rn==1").drop("rn")

            df_rating_stats_with_movie = movies.join(df, movies.MovieID ==  df.MovieID, "left").select( \
                movies.MovieID, F.col("Title"), F.col("Genres"), F.col("MinRating"), F.col("MaxRating"), F.col("AvgRating")).orderBy(movies.MovieID)
        except Exception as exp:
            logger.error(str(exp))
            raise
        return df_rating_stats_with_movie


    # Get any top 3 movies of each user based on user's ratings for the movies 
    def get_each_user_top_movies(self, movies: DataFrame, ratings: DataFrame) -> DataFrame:
        try:
            window = Window.partitionBy(ratings['UserID']).orderBy(ratings['Rating'].desc())
            df_user_top_ratings = ratings.select('*', F.row_number().over(window).alias('RatingNumber')).filter(F.col('RatingNumber') <= 3)

            df_user_top_ratings_moview = movies.join(df_user_top_ratings, \
                movies.MovieID ==  df_user_top_ratings.MovieID, "inner").select( \
                F.col("UserID"), movies.MovieID, F.col("Title"), F.col("Rating"), F.col("RatingNumber")).orderBy([F.col("UserID"), F.col("RatingNumber")])

        except Exception as exp:
            logger.error(str(exp))
            raise
        return df_user_top_ratings_moview        

    # Write dataframe for reuired format suitable for the dataware house  
    def write_dataframe_into_file(self, df_source: DataFrame, file_name: str, file_path: str, file_format: str = "parquet", file_mode: str = "overwrite"):
        try:
            if file_format == key.FILE_FORMAT_CSV.value:  
                file = file_path + "/df_csv/" + file_name
                self.write_csv(df_source, file, "false")
            elif file_format == key.FILE_FORMAT_ORC.value:
                file = file_path + "/df_orc/" + file_name
                self.write_orc(df_source, file)
            else :
                file = file_path + "/df_parquet/" + file_name
                self.write_parquet(df_source, file) 
                
            logger.info("Dataframe has been written successfully as {} in the file format {}".format(file_name, file_format))
        except Exception as exp:
            logger.error(str(exp))
            raise 

    # Movie stats -  get all movie data and add ratings's stats if available
    # Same function via SQL query
    def get_movie_rating_stats_sql(self, movies: DataFrame, ratings: DataFrame) -> DataFrame:
        try:
            movies.createOrReplaceTempView("movie")
            ratings.createOrReplaceTempView("ratings")
            df_rating_stats_with_movie = self.spark.sql(model.sql_query_movie_stats)            
        except Exception as exp:
            logger.error(str(exp))
            raise

        return df_rating_stats_with_movie

    # Get any top 3 movies of each user based on user's ratings for the movies 
    # Same function via SQL query
    def get_each_user_top_movies_sql(self, movies: DataFrame, ratings: DataFrame) -> DataFrame:
        try:
            movies.createOrReplaceTempView("movie")
            ratings.createOrReplaceTempView("ratings")
            df_top_movie = self.spark.sql(model.sql_query_user_top_movies)            
        except Exception as exp:
            logger.error(str(exp))
            raise

        return df_top_movie



"""run() method to execute the job pipeline"""
def run(job_config):
    try:
        # Instantiate 
        job = PySparkJobMovieLens(job_config["job_name"])
        logger.info("Start of Job {} has started.".format(job_config["job_name"]))

        ################## Extraction & cleaning ####################################
        logger.info("1. Read movie and raings into dataframes")
        df_movies   = job.read_movie_data(job_config)
        df_ratings  = job.read_rating_data(job_config)
        df_users    = job.read_user_data(job_config)

        logger.info("Count of movie dataset {}.".format(df_movies.count()))
        logger.info("Count of rating dataset {}.".format(df_ratings.count()))
        logger.info("Count of user dataset {}.".format(df_users.count()))


        logger.info("Check nulls and NaN for movie dataframe") 
        null_check = DataValidation.check_null(df_movies, "MovieID")
        if null_check == 0 :
            logger.info("Movie dataframe is okay.") 
        else:
            logger.info("Perform clenup on movie dataframe.") 

        logger.info("End task 1")
        #############################################################################

        ################## Transformation ###########################################
        logger.info("2. Creates a new dataframe, which contains the movies data and 3 new columns max, min and average rating for that movie from the ratings data.")
        df_movies_rating_agg = job.get_movie_rating_stats_sql(df_movies, df_ratings)
        #df_movies_rating_agg = job.get_movie_rating_stats(df_movies, df_ratings) # By using pyspark sql functions
        df_movies_rating_agg.show(5)
        logger.info(df_movies_rating_agg.count())
        logger.info("End of task 2")
        ######################################################

        ######################################################
        logger.info("3. Create a new dataframe which contains each user’s (userId in the ratings data) top 3 movies based on their rating.")
        df_user_top_ratings_movie = job.get_each_user_top_movies_sql(df_movies, df_ratings)
        #df_user_top_ratings_movie = job.get_each_user_top_movies(df_movies, df_ratings)  # By using pyspark sql functions
        df_user_top_ratings_movie.show(10)
        print(df_user_top_ratings_movie.count())
        logger.info("End of task 3")
        ##############################################################################

        ################# Save for Data warehouse#####################################
        logger.info("4. Write out the original and new dataframes in an efficient format.")
        # Save DF in the required format
        job.write_dataframe_into_file(df_movies, "ori_movies", job_config["output_path"], key.FILE_FORMAT_CSV.value)
        job.write_dataframe_into_file(df_movies, "ori_movies", job_config["output_path"], key.FILE_FORMAT_PARQUET.value)
        job.write_dataframe_into_file(df_movies, "ori_movies", job_config["output_path"], key.FILE_FORMAT_ORC.value)

        # Save DF in parquet and good for Snowflake
        job.write_dataframe_into_file(df_ratings, "ori_ratings", job_config["output_path"])
        job.write_dataframe_into_file(df_users, "ori_users", job_config["output_path"])

        job.write_dataframe_into_file(df_movies_rating_agg, "movies_stats", job_config["output_path"])
        job.write_dataframe_into_file(df_user_top_ratings_movie, "user_top_movies", job_config["output_path"])

        logger.info("End of task 4")
        ###############################################################################

        logger.info("End of Job {}.".format(job_config["job_name"]))
    except Exception as exp :
        raise
        # kill the spark object
        job.stop()
