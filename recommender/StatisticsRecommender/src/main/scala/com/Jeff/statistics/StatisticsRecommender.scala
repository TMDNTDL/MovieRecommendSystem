package com.Jeff.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Case Class
 */

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

case class Rating(uid:Int, mid: Int, score: Double, timestamp: Int)
case class MongoConfig(uri:String, db:String)
// define a base recommendMovieObject
case class Recommendation( mid: Int, score: Double)
// define Movie type top10 recommend object
case class GenresRecommendation( genres: String, resc: Seq[Recommendation] )
object StatisticsRecommender {
  // Table in mongoDB going for search

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  // Res table
  val RATE_MORE_MOVIE = "RateMOreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb:localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")

    // create a spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // Loading Data from MongoDB
    val ratingDF = spark.read
      .option("spark.mongodb.read.connection.uri", mongoConfig.uri)
      .option("database", mongoConfig.db)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("mongodb")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = spark.read
      .option("spark.mongodb.read.connection.uri", mongoConfig.uri)
      .option("database", mongoConfig.db)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("mongodb")
      .load()
      .as[Movie]
      .toDF()

    // create temp View for rating
    ratingDF.createOrReplaceTempView("ratings")

    //TODO: find result for different recommend method
    // 1. historical famous movie based on grade point
    // 2. recent famous movie
    // 3. avg movie grade
    // 4. Top 10 for different tags of movie

    spark.stop()
  }

}
