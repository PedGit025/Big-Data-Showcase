from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, year, month, count, length, avg, stddev, min, max, desc

# Initialize SparkSession
spark = SparkSession.builder.appName('AwsEmrSpark').getOrCreate()

# Input and output paths
input_path = 's3://bucket/movie_reviews/input/movie_review.csv'
output1 = 's3://bucket/movie_reviews/output/top_100_words'
output2 = 's3://bucket/movie_reviews/output/bottom_10'
output3 = 's3:/bucket/movie_reviews/output/reviews_per_year'
output4 = 's3://buckett/movie_reviews/output/review_length_stats'
output5 = 's3://bucket/movie_reviews/output/title_rating_stats'
output6 = 's3://bucket/movie_reviews/output/rating_distribution'
output7 = 's3://bucket/movie_reviews/output/rating_stats'
output8 = 's3://bucket/movie_reviews/output/highest_rated_movies'
output9 = 's3://bucket/movie_reviews/output/most_reviewed_movies'
output10 = 's3://bucket/movie_reviews/output/year_highest_rated'
output11 = 's3://bucket/movie_reviews/output/year_most_reviews'
output12 = 's3://bucket/movie_reviews/output/longest_review_per_year'
output13 = 's3://bucket/movie_reviews/output/yearly_avg_rating'
output14 = 's3://bucket/movie_reviews/output/yearly_rating_distribution'
output15 = 's3://bucket/movie_reviews/output/review_word_count_stats'
output16 = 's3://bucket/movie_reviews/output/combined_analysis1'
output17 = 's3://bucket/movie_reviews/output/combined_analysis2'
output18 = 's3://bucket/movie_reviews/output/combined_analysis3'
output19 = 's3://bucket/movie_reviews/output/combined_analysis4'
output20 = 's3://bucket/movie_reviews/output/combined_analysis5'

def main():
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df.cache()  # Cache DataFrame for better performance if needed
    df.createOrReplaceTempView("movie_reviews")  # Create a temporary view for SQL operations
    
    # Select col
    reviews = df.select('Review')
    
    # Map words via split in col Review
    words = reviews.select(explode(split(col('Review'), '\\s+')).alias('word'))

    # Exclude empty cells
    words = words.filter(words.word != '')

    # Prep for map
    word_count = words.groupBy('word').count()

    # Reduce
    top_100_words = word_count.orderBy(col('count').desc()).limit(100)
    bot_10 = word_count.orderBy(col('count').asc()).limit(10)
    
    reviews_per_year = df.groupBy('Year').count()

    # Calculate review lengths
    review_lengths = df.withColumn('Review_Length', length('Review'))
    length_stats = review_lengths.agg(
        avg('Review_Length').alias('Avg_Review_Length'),
        stddev('Review_Length').alias('StdDev_Review_Length'),
        min('Review_Length').alias('Min_Review_Length'),
        max('Review_Length').alias('Max_Review_Length')
    )

    # Calculate average rating and review count per title
    title_stats = df.groupBy('Title').agg(
        avg('Rating').alias('Avg_Rating'),
        stddev('Rating').alias('StdDev_Rating'),
        count('Rating').alias('Review_Count')
    ).orderBy(desc('Review_Count')).limit(100)

    # Calculate rating distribution
    rating_distribution = df.groupBy('Rating').count().orderBy('Rating')

    # Calculate overall rating statistics
    rating_stats = df.agg(
        avg('Rating').alias('Avg_Rating'),
        stddev('Rating').alias('StdDev_Rating'),
        min('Rating').alias('Min_Rating'),
        max('Rating').alias('Max_Rating')
    )

    # Find highest rated movies
    highest_rated_movies = df.groupBy('Title').agg(avg('Rating').alias('Avg_Rating')).orderBy(desc('Avg_Rating')).limit(100)

    # Find most reviewed movies
    most_reviewed_movies = df.groupBy('Title').agg(count('Review').alias('Review_Count')).orderBy(desc('Review_Count')).limit(100)

    # Find highest rated year
    year_highest_rated = df.groupBy('Year').agg(avg('Rating').alias('Avg_Rating')).orderBy(desc('Avg_Rating')).limit(10)

    # Find year with most reviews
    year_most_reviews = df.groupBy('Year').count().orderBy(desc('count')).limit(10)

    # Find longest review per year
    longest_review_per_year = review_lengths.groupBy('Year').agg(max('Review_Length').alias('Max_Review_Length'))

    # Calculate yearly average rating
    yearly_avg_rating = df.groupBy('Year').agg(avg('Rating').alias('Avg_Rating')).orderBy('Year')

    # Calculate yearly rating distribution
    yearly_rating_distribution = df.groupBy('Year', 'Rating').count().orderBy('Year', 'Rating')

    # Calculate word count statistics
    word_count_stats = words.groupBy('word').agg(count('word').alias('Word_Count')).agg(
        avg('Word_Count').alias('Avg_Word_Count'),
        stddev('Word_Count').alias('StdDev_Word_Count'),
        min('Word_Count').alias('Min_Word_Count'),
        max('Word_Count').alias('Max_Word_Count')
    )

    # Perform combined analysis
    combined_analysis1 = top_100_words.join(word_count_stats, 'word').select('word', 'count', 'Avg_Word_Count', 'StdDev_Word_Count')
    combined_analysis2 = yearly_avg_rating.join(length_stats)
    combined_analysis3 = highest_rated_movies.join(title_stats, 'Title').select('Title', 'Avg_Rating', 'Review_Count', 'StdDev_Rating')
    combined_analysis4 = longest_review_per_year.join(length_stats)
    combined_analysis5 = reviews_per_year.join(yearly_rating_distribution, 'Year').select('Year', 'count', 'Rating', 'Avg_Rating')

    # Saving results to S3
    top_100_words.write.csv(output1, mode='overwrite')
    bot_10.write.csv(output2, mode='overwrite')
    reviews_per_year.write.csv(output3, mode='overwrite')
    length_stats.write.csv(output4, mode='overwrite')
    title_stats.write.csv(output5, mode='overwrite')
    rating_distribution.write.csv(output6, mode='overwrite')
    rating_stats.write.csv(output7, mode='overwrite')
    highest_rated_movies.write.csv(output8, mode='overwrite')
    most_reviewed_movies.write.csv(output9, mode='overwrite')
    year_highest_rated.write.csv(output10, mode='overwrite')
    year_most_reviews.write.csv(output11, mode='overwrite')
    longest_review_per_year.write.csv(output12, mode='overwrite')
    yearly_avg_rating.write.csv(output13, mode='overwrite')
    monthly_rating_distribution.write.csv(output14, mode='overwrite')
    word_count_stats.write.csv(output15, mode='overwrite')
    combined_analysis1.write.csv(output16, mode='overwrite')
    combined_analysis2.write.csv(output17, mode='overwrite')
    combined_analysis3.write.csv(output18, mode='overwrite')
    combined_analysis4.write.csv(output19, mode='overwrite')
    combined_analysis5.write.csv(output20, mode='overwrite')

    spark.stop()

if __name__ == '__main__':
    main()
