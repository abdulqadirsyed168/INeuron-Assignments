from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from os.path import abspath
import logging 
from pyspark.sql.functions import *
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
warehouse_location = abspath('spark-warehouse') 

spark = SparkSession.builder.master("spark://localhost:7077").appName("demo").config("spark.sql.warehouse.dir", warehouse_location).enableHiveSupport().getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.setLogLevel("INFO")
spark.sparkContext.setLogLevel("WARN")


def movies_data():
    logging.info("Reading data from movies and programatically specified the schema")
    movies_df1 = spark.read.format("csv").option("inferSchema", True).option("header", False).option("delimiter", "::").load("hdfs:///data/movies.dat")
    logging.info("Reading schema from movies")
    movies_df1.printSchema()
    movies_df1.show(5,False)
    logging.info("renaming columns names as mentioned in the dataset documentation")
    global movies_df2
    movies_df2 =  movies_df1.selectExpr('_c0 AS MovieId','_c1 AS Title','_c2 AS Genres')
    logging.info("Reading new schema from movies")
    movies_df2.printSchema()
    movies_df2.show(5,False)
    logging.info("Converting Genres in to array type")
    movies_df3 = movies_df2.withColumn('Genres', F.regexp_replace("Genres", '[|]', ',').alias('new_name'))
    movies_df3.show(5,False)
    global movies_df4
    movies_df4 = movies_df3.select(col("MovieId"),col("Title"),split(col("Genres"),",").alias("Genres"))
    movies_df4.printSchema()
    movies_df4.show(5,False)
    logging.info("Saving movies Tables without defining DDL")
    movies_df2.write.mode('overwrite').saveAsTable("movies")
    spark.sql("show tables in default").show()

def ratings_data():
    logging.info("Reading data from ratings and programatically specified the schema")
    ratings_df1 = spark.read.format("csv").option("inferSchema", True).option("header", False).option("delimiter", "::").load("hdfs:///data/ratings.dat")
    logging.info("Reading schema from ratings")
    ratings_df1.printSchema()
    ratings_df1.show(5,False)
    logging.info("renaming columns names as mentioned in the dataset documentation")
    global ratings_df2
    ratings_df2 = ratings_df1.selectExpr('_c0 AS UserID','_c1 AS MovieID','_c2 AS Rating','_c3 AS  Timestamp')
    logging.info("Reading new schema from ratings")
    ratings_df2.printSchema()
    ratings_df2.show(5,False)
    logging.info("Converting Epoch time format to Readable Time format")
    ratings_df3 = ratings_df2.select(col("*"),from_unixtime(col("Timestamp"),"MM-dd-yyyy HH:mm:ss").alias("Timestamp_1"))
    ratings_df3.show(5,False)
    logging.info("Saving movies Tables without defining DDL")
    ratings_df2.write.mode('overwrite').saveAsTable("ratings")
    spark.sql("show tables in default").show()

def users_data():
    logging.info("Reading data from users and programatically specified the schema")
    users_df1 = spark.read.format("csv").option("inferSchema", True).option("header", False).option("delimiter", "::").load("hdfs:///data/users.dat")
    logging.info("Reading schema from users")
    users_df1.printSchema()
    users_df1.show(5,False)
    logging.info("renaming columns names as mentioned in the dataset documentation")
    global users_df2
    users_df2 = users_df1.withColumnRenamed("_c0","UserID")\
    .withColumnRenamed("_c1","Gender")\
    .withColumnRenamed("_c2","Age")\
    .withColumnRenamed("_c3","Occupation")\
    .withColumnRenamed("_c4","Zip_code")
    logging.info("Reading new schema from users")
    users_df2.printSchema()
    users_df2.show(5,False)
    logging.info("Specifiying meaning of Occupation as Occupation_Describe ")
    users_df3 = users_df2.withColumn("Occupation_Describe",when(col("Occupation") == 0, "other or not specified")
               .when(col("Occupation") == 1 , "academic/educator")
               .when(col("Occupation") == 2 , "artist")
               .when(col("Occupation") == 3 , "clerical/admin")
               .when(col("Occupation") == 4 , "college/grad student")
               .when(col("Occupation") == 5 , "customer service")
               .when(col("Occupation") == 6 , "doctor/health care")
               .when(col("Occupation") == 7 , "executive/managerial")
               .when(col("Occupation") == 8 , "farmer")
               .when(col("Occupation") == 9 , "homemaker")
               .when(col("Occupation") == 10 , "K-12 student")
               .when(col("Occupation") == 11 , "lawyer")
               .when(col("Occupation") == 12 , "programmer")
               .when(col("Occupation") == 13 , "retired")
               .when(col("Occupation") == 14 , "sales/marketing")
               .when(col("Occupation") == 15 , "scientist")
               .when(col("Occupation") == 16 , "self-employed")
               .when(col("Occupation") == 17 , "technician/engineer")
               .when(col("Occupation") == 18 , "tradesman/craftsman")
               .when(col("Occupation") == 19 , "unemployed")
               .otherwise("writer"))
    users_df3.show(5,False)
    logging.info("Specifiying Age column range as mention in Documentation ")
    users_df4 = users_df3.withColumn("Age_Range",when(col("Age") == 1, "Under 18")
                     .when(col("Age") == 18, "18-24")
                     .when(col("Age") == 25, "25-34")
                     .when(col("Age") == 35, "35-44")
                     .when(col("Age") == 45, "45-49")
                     .when(col("Age") == 50, "50-55")
                     .otherwise("56+")
                    )
    users_df4.show(5,False)
    logging.info("Saving movies Tables without defining DDL")
    users_df2.write.mode('overwrite').saveAsTable("users")
    spark.sql("show tables in default").show()


def broadcast_variable():
    movies_data()
    ratings_data()
    users_data()
    logging.info("broadcast variable example (i.e., joining  ratings and movies datasets )")
    global ratings_movie_join
    ratings_movie_join = ratings_df2.join(broadcast(movies_df2), ratings_df2['MovieID']==movies_df2['MovieId'],'inner').drop(movies_df2['MovieId'])
    ratings_movie_join.show(5,False)

def Analytical_Queries():
    logging.info("#####Spark dataframe queries#######")
    broadcast_variable()
    logging.info("######Spark Analytical queries######")
    logging.info(" top 10 most viewed movies")
    top_view=ratings_movie_join.groupby("MovieID",'Title').agg(count("*").alias("top_viewed_movies")).sort(desc("top_viewed_movies"))
    top_view.show(10)

    logging.info(" distinct list of genres available")
    movies_df4.select("Genres").distinct().show()
    # movies_df4.groupBy("Genres").show()

    logging.info("number of  movies for each genre")
    movies_df4.groupBy("Genres").count().orderBy(col("count").desc()).show()
    movies_df4.groupBy("Genres").agg(count("*").alias("no_of_movies")).orderBy(col("no_of_movies").desc()).show(10,False)

    logging.info("number of movies are starting with numbers or letters (Example: Starting with 1/2/3../A/B/C..Z)")
    print(' ')
    number = movies_df4.filter(movies_df4["Title"].rlike("^[0-9]"))
    print("number of movies starts with NUMBERS : ",number.count())

    alphabets = movies_df4.filter(~movies_df4["Title"].rlike("^[0-9]"))
    print("number of movies starts with LETTERS : ",alphabets.count())

    logging.info("List of latest released movies")
    df = movies_df4.withColumn("year",expr("right(title, 6)"))
    df.select(col("Title"),col("year")).orderBy(col("year").desc()).show(10,False)


def Spark_SQL_Queries():
    Analytical_Queries()
    logging.info("#### Spark SQL Queries ###")
    logging.info("list of the oldest released movies")
    movies_df2.createOrReplaceTempView("movies")
    sqlDF = spark.sql(" SELECT title,REGEXP_REPLACE(right(title,5) , '[^\\x20-\\x7E]', '') as year from movies order by year asc ")
    sqlDF.show()
    logging.info("number of  movies are released each year")
    sqlDF = spark.sql(" select count(p.title) as movies_per_year , p.year  from (SELECT *,REGEXP_REPLACE(right(title,5) , '[^\\x20-\\x7E]', '') as year FROM movies order by year asc) as p group by p.year order by movies_per_year desc ")
    sqlDF.show() 

    logging.info("number of movies are there for each rating")
    ratings_df2.createOrReplaceTempView("ratings")
    sqlDF = spark.sql(" select count(MovieID) as number_of_movies , Rating from ratings group by Rating ")
    sqlDF.show()
    logging.info("number of users have rated each movie")
    ratings_movie_join.createOrReplaceTempView("ratings")
    sqlDF = spark.sql(" select count(UserID) as number_of_users_rated , Title from ratings group by Title order by number_of_users_rated  desc")
    sqlDF.show(10,False)
    logging.info(" total ratings for each movie")
    sqlDF = spark.sql(" select sum(Rating) as total_rating , Title from ratings group by Title order by total_rating  desc")
    sqlDF.show(10,False)
    logging.info(" average ratings for each movie")
    sqlDF = spark.sql(" select round(avg(Rating),2) as average_rating , Title from ratings group by Title order by average_rating  desc")
    sqlDF.show(10,False)

if __name__ == '__main__':
    movies_data()
    ratings_data()
    users_data()
    Analytical_Queries()
    Spark_SQL_Queries()
        




     










