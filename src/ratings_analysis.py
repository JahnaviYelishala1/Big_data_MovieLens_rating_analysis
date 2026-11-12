
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, explode, split
import os

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("MovieLensRatingAnalysis") \
    .master("local[*]") \
    .getOrCreate()

# 2. Read ratings data from HDFS
ratings = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("hdfs://localhost:9000/movielens/ratings.csv")

# 3. Display schema
print("Dataset Schema:")
ratings.printSchema()

# 4. Calculate total ratings
total_ratings = ratings.count()
print("\nTotal Ratings:", total_ratings)

# 5. Calculate average rating
summary = ratings.agg(
    avg("rating").alias("average_rating")
)

print("\nOverall Average Rating:")
summary.show()

# 6. Find total ratings given by each user
top_users = ratings.groupBy("userId") \
    .agg(count("rating").alias("total_ratings")) \
    .orderBy(col("total_ratings").desc())

print("\nTop 10 Most Active Users:")
top_users.show(10)

# 7. Read movies data from HDFS
movies = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("hdfs://localhost:9000/movielens/movies.csv")

# 8. Join movies with ratings
movie_ratings = ratings.join(
    movies, on="movieId", how="inner"
)

# 9. Calculate average rating and rating count
movie_stats = movie_ratings.groupBy("movieId", "title") \
    .agg(
        avg("rating").alias("avg_rating"),
        count("rating").alias("rating_count")
    )

# 10. Filter movies with at least 50 ratings
top_movies = movie_stats.filter(
    col("rating_count") >= 50
).orderBy(col("avg_rating").desc())

# 11. Display top 10 highest-rated movies
print("\nTop 10 Highest Rated Movies (Minimum 50 Ratings):")
top_movies.show(10, truncate=False)

# 12. Genre-wise analysis
genre_data = movie_ratings.withColumn(
    "genre",
    explode(split(col("genres"), r"\|"))
).filter(col("genre") != "(no genres listed)")

genre_stats = genre_data.groupBy("genre") \
    .agg(
        avg("rating").alias("avg_rating"),
        count("rating").alias("rating_count")
    ) \
    .orderBy(col("avg_rating").desc())

print("\nGenre-wise Average Ratings:")
genre_stats.show(20, truncate=False)

# 13. Rating distribution
rating_distribution = ratings.groupBy("rating") \
    .agg(
        count("rating").alias("total_ratings")
    ) \
    .orderBy(col("rating").asc())

print("\nRating Distribution:")
rating_distribution.show(10)

# 14. Export results for Streamlit dashboard
output_dir = os.path.expanduser(
    "~/MovieLensProject/data/processed"
)

os.makedirs(output_dir, exist_ok=True)

# Export summary
summary.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{output_dir}/summary")

# Export top users
top_users.limit(10).coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{output_dir}/top_users")

# Export top movies
top_movies.limit(100).coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{output_dir}/top_movies")

# Export genre statistics
genre_stats.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{output_dir}/genre_stats")

# Export rating distribution
rating_distribution.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{output_dir}/rating_distribution")

print("\nAll analytics results exported successfully!")
print("Output directory:", output_dir)

# 15. Stop Spark session
spark.stop()
