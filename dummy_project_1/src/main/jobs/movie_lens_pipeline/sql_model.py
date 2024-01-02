# Creates a new dataframe, which contains the movies data and 3 new columns max, min and
# average rating for that movie from the ratings data.
sql_query_movie_stats = '''
    WITH movie_stats AS (
        SELECT 
            MovieID
            , MIN(Rating) AS MinRating
            , MAX(Rating) AS MaxRating
            , AVG(Rating) AS AvgRating
        FROM 
            ratings
        GROUP BY 
            MovieID
    )
    SELECT 
        m.MovieID AS MovieID
        , m.Title AS Title
        , m.Genres AS Genres
        , r.MinRating
        , r.MaxRating
        , r.AvgRating
    FROM 
        movie m
    LEFT JOIN
        movie_stats r
        ON m.MovieID = r.MovieID
    ORDER BY 
        MovieID
'''

# Create a new dataframe which contains each user’s (userId in the ratings data) top 3 movies
# based on their rating.
# Assumption - To get any ONLY top 3 movies for each user as rated by the user with hightest to lowest.
sql_query_user_top_movies = '''
    WITH user_top_movies AS (
        SELECT 
            UserID
            , MovieID
            , Rating
            , ROW_NUMBER() OVER(PARTITION BY UserID ORDER BY Rating DESC) AS RatingNumber
        FROM 
            ratings
    )
    SELECT 
        t.UserID
        , m.MovieID
        , m.Title
        , t.Rating
        , t.RatingNumber
    FROM
        movie m 
    INNER JOIN
        user_top_movies t
        ON m.MovieID = t.MovieID
    WHERE 
        t.RatingNumber <= 3
    ORDER BY 
        UserID
        , RatingNumber
'''
