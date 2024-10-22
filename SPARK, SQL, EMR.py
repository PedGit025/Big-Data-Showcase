from pyspark.sql import SparkSession
from pyspark.sql.functions import year, avg, count, max, length, split, explode

spark = SparkSession.builder.appName('EmrSql').getOrCreate()

input = 's3://BUCKETNAME/movie_reviews/input/movie_review.csv'
output = 's3://BUCKETNAME/movie_reviews/output/'

def main():
    # Read CSV
    df = spark.read.csv(input, header=True, inferSchema=True)
    # Convert RDD to DataFrame to execute SQL
    df.createOrReplaceTempView('Reviews')
    
    # SQL queries
    
    # Count
    record_count = spark.sql('SELECT COUNT(*) AS Total_Count FROM Reviews')
    record_count.write.csv(f'{output}total_count.csv', header=True)
    
    # Average rating
    ave_rate = spark.sql('SELECT AVG(Rating) AS ave_rate FROM Reviews')
    ave_rate.write.csv(f'{output}ave_rate.csv', header=True)
    
    # Review count per rating
    rating_count = spark.sql('SELECT Rating, COUNT(*) AS rating_count FROM Reviews GROUP BY Rating')
    rating_count.write.csv(f'{output}rating_count.csv', header=True)
    
    # Number of Reviews by Year
    reviews_per_year = spark.sql('SELECT YEAR(Review_Date) AS Review_Year, COUNT(*) AS Reviews_Per_Year FROM Reviews GROUP BY YEAR(Review_Date)')
    reviews_per_year.write.csv(f'{output}reviews_per_year.csv', header=True)
    
    # Most Common Words in Titles
    top_words_in_titles = spark.sql("""
    SELECT word, COUNT(*) AS word_count
    FROM (
        SELECT explode(split(Title, ' ')) AS word
        FROM Reviews
    )
    GROUP BY word
    ORDER BY word_count DESC
    LIMIT 10""")
    top_words_in_titles.write.csv(f'{output}top_words_in_titles.csv', header=True)
    
    # Average Length of Reviews
    average_review_length = spark.sql("SELECT AVG(LENGTH(Review)) AS Average_Review_Length FROM Reviews")
    average_review_length.write.csv(f'{output}average_review_length.csv', header=True)
    
    spark.stop()

if __name__ == "__main__":
    main()
