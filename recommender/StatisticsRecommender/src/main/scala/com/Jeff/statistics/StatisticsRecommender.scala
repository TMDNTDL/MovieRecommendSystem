package com.Jeff.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date

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
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
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
    val rateMoreMoviesDF = spark.sql("select mid, count(mid) as count from ratings group by mid")
    // write result into table
    storeDFInMongoDB( rateMoreMoviesDF, RATE_MORE_MOVIE )

    // 2. recent famous movie
    // create a dateFormat tools
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")

    // register udf, turn time tick into year/month format
    spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)

    // preprocess the rating table, convert time tick into DataFormat
    val ratingOfYearMonth = spark.sql("select mid, score, changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

    // from rating search movie in every month's rating. mid, count, yearmonth
    val ratingMoreRecentlyMoviesDF = spark.sql("select mid, count(mid) as count, yearmonth from ratingOfMonth group by yearmonth, mid order by yearmonth desc, count desc")

    // store res
    storeDFInMongoDB(ratingMoreRecentlyMoviesDF, RATE_MORE_RECENTLY_MOVIES )

    // 3. avg movie grade
    val averageMoviesDF = spark.sql("select mid, avg(score) as avg from ratings group by mid order by avg")
    storeDFInMongoDB(averageMoviesDF, AVERAGE_MOVIES)


    // 4. Top 10 for different tags of movie
    //define all the type
    val genres =
      List("Action","Adventure","Animation","Comedy","Crime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
    ,"Romance","Science","Tv","Thriller","War","Western")
    // add avg score to an another column in movie table, inner join
    val movieWithScore = movieDF.join(averageMoviesDF, "mid")

    // full outer-join, convert genres into RDD
    val genresRDD = spark.sparkContext.makeRDD(genres)

    val genresTopMoviesDF = genresRDD.cartesian(movieWithScore.rdd)
      .filter{
        // if genres field in movie table(Action|Adventure|Sci-Fri) include the genresRDD value, that mean this movie count as this genres (Action)
        case (genres, row) => row.getAs[String]("genres").toLowerCase().contains( genres.toLowerCase())
      }
      .map{
        case (genres, movieRow) => ( genres, ( movieRow.getAs[Int]("mid"), movieRow.getAs[Double]("avg")))
      }
      .groupByKey()
      .map{
        case (genre, items) => GenresRecommendation( genre, items.toList.sortWith(_._2>_._2).take(10).map( item=>
          Recommendation(item._1, item._2)
        ))
      }
      .toDF()

    storeDFInMongoDB(genresTopMoviesDF, GENRES_TOP_MOVIES)

    spark.stop()
  }


  def storeDFInMongoDB(df: DataFrame, collectionName: String)(implicit mongoConfig:MongoConfig):Unit = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("spark.mongodb.database", mongoConfig.db)
      .option("collection", collectionName)
      .mode("overwrite")
      .format("mongodb")
      .save()
  }

}
