package com.Jeff

import org.apache.spark.sql.ForeachWriter
import com.mongodb.client.MongoClients
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

// Recommendation case Object
case class Recommendation (mid: Int, score: Double)

// define User recommendation case class
case class UserRecs( uid: Int, recs: Seq[Recommendation])

// based on LFM movie characteristic vector, movie similarity list
case class MovieRecs( mid: Int, Recs: Seq[Recommendation])
case class MongoConfig(uri:String, db:String)

// Define connector Helper
object ConnHelper extends Serializable{
  // only run when need to use
  lazy val jedis = new Jedis("localhost")
  lazy val mongoClient = MongoClients.create("mongodb://localhost:27017/recommender")
}
object RealTimeRecommender {
  val MAX_USER_RATING_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"

    )
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("RealTimeRecommender")

    // create a spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // streaming context
    val sc = spark.sparkContext

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // Loading movie similarity matrix
    val simMovieMatrix = spark.read
      .option("spark.mongodb.read.connection.uri", mongoConfig.uri)
      .option("database", mongoConfig.db)
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("mongodb")
      .load()
      .as[MovieRecs]
      .rdd
      .map{(
        // 为了查询相似度方便，转换成map
        // Recs.toMap invalid, Recs = Seq[Recommendation(Int, Double)], toMap require [K,V]
        movieRecs => (movieRecs.mid, movieRecs.Recs.map( x => (x.mid, x.score)).toMap)
      )}.collectAsMap() // collectAsMap pull RDD back to driver, for easy searching

    val simMovieMatrixBroadCast = sc.broadcast(simMovieMatrix)


    // Connect Kafka Stream to Spark Stream
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", config("kafka.topic")) // subscribe topic
      .option("startingOffsets", "latest")
      .option("group.id", "recommender")
      .load()

    // Turn Binary into String
    val rawRatings = kafkaStream.selectExpr("CAST(key AS STRING)", "CAST(Value AS STRING)").as[String]

    // 把原始数据UID|MID|SCORE|TIMESTAMP 转换成评分流
    val ratingStream = rawRatings.map{
      msg =>
        val attr = msg.split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }

    val writer = new ForeachWriter[(Int, Int, Double, Int)]{
      override def open(partitionId: Long, version: Long): Boolean = true

      override def process(record: (Int, Int, Double, Int)):Unit ={
        val (uid, mid, score, timestamp) = record

        println("rating data coming! >>>>>>>>>>>>>>>>>>>>>>>")

        // get recently M movie score
        val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATING_NUM, uid, ConnHelper.jedis)

        // get K similar movie to Movie P, as a backed up
        val simMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadCast.value)

        // calculate movie recommendation level
        val streamRecs = computeMovieScores(simMovieMatrixBroadCast.value, userRecentlyRatings, simMovies)

        // save Data to MongoDB
        saveRecsToMongoDB(uid, streamRecs)
      }

      override def close(errorOrNull:  scala.Throwable): Unit = {}
    }

    // Core Algorithms
    val query = ratingStream.writeStream
      .foreach(writer)
      .outputMode("append")
      .start()

    println(">>>>>>>>>>>>>>>>>>>>>>> streaming started!")
    query.awaitTermination()
  }
  import scala.jdk.CollectionConverters._
  def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {
    // read from redis, Key=uid:UID, value = MID:SCORE
    jedis.lrange("uid:" + uid, 0, num-1).asScala
      .map{
        item =>
          val attr = item.split(":")
          ( attr(0).trim.toInt, attr(1).trim.toDouble)
      }
      .toArray
  }

  /**
   * Get num similar movie to current movie, as a backed up
   * @param num num of similar movie
   * @param mid current movie ID
   * @param uid current user ID
   * @param simMovies similar matrix
   * @return filter out the backed up movie list
   */
  def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                     (implicit mongoConfig: MongoConfig) : Array[Int] ={
    // 1. get all similar movie from the similar matrix

    // 2. get viewed movie of user from mongodb

    // 3. filter, get the output
  }
}
