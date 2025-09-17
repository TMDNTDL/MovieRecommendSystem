package com.Jeff

import org.apache.spark.sql.ForeachWriter
import com.mongodb.client.MongoClients
import com.mongodb.client.model.Filters
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.bson.Document
import redis.clients.jedis.Jedis

import java.util
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

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
  lazy val jedis = new Jedis("172.18.41.215", 6379)
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
    val kafkaStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.18.41.215:9092")
      .option("subscribe", "recommender") // subscribe topic
      .option("startingOffsets", "latest")
      .load()

    // Turn Binary into String
    val rawRatings = kafkaStream.selectExpr("CAST(value AS STRING)").as[String]

    println("Ready to take data!!!")
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
        val streamRecs = computeMovieScores(simMovies, userRecentlyRatings, simMovieMatrixBroadCast.value)

        // save Data to MongoDB
        saveDataToMongoDB(uid, streamRecs)
      }

      override def close(errorOrNull:  scala.Throwable): Unit = {}
    }

    val debugStream = ratingStream.map { record =>
      println(s"Received record: $record")
      record
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
    val allSimMovies = simMovies(mid).toArray

    // 2. get viewed movie of user from mongodb
    val ratingExistCollection = ConnHelper.mongoClient
      .getDatabase(mongoConfig.db)
      .getCollection(MONGODB_RATING_COLLECTION)

    val ratingExist = ratingExistCollection
      .find(Filters.eq("uid", uid))
      .into(new util.ArrayList[Document]())
      .asScala
      .map(doc => doc.get("mid").toString.toInt)
      .toArray
    // 3. filter, get the output

    allSimMovies.filter( x => !ratingExist.contains(x._1))
      .sortWith(_._2> _._2)
      .take(num)
      // (mid, score)
      .map(x => x._1) // taken out the score and only mid
  }

  def computeMovieScores(CandidateMovies: Array[Int],
                         userRecentlyRatings: Array[(Int, Double)],
                         simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Array[(Int, Double)] ={
    // ArrayBuffer, stores basic score for all candidate movie
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()

    // define HashMap, store all candidate and its UserRecentlyRatings
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val descreMap = scala.collection.mutable.HashMap[Int, Int]()

    for ( candidateMovie <- CandidateMovies; userRecentlyRatings <- userRecentlyRatings){
      // get candidate movie and its comparison of rated movie.
      val simScore = getMoviesSimScore( candidateMovie, userRecentlyRatings._1, simMovies)

      if (simScore > 0.7){
        scores += ((candidateMovie, simScore * userRecentlyRatings._2))
        if ( userRecentlyRatings._2 > 3){
          increMap(candidateMovie) = increMap.getOrElseUpdate(candidateMovie, 0) + 1
        }else{
          descreMap(candidateMovie) = descreMap.getOrElseUpdate(candidateMovie, 0) + 1
        }
      }
    }

    // groupby mid for candidate movie
    scores.groupBy(_._1).map{
          // after groupby, return an Map( mid -> ArrayBuffer[(mid, score)])
      case (mid, scoreList) =>
        ( mid, scoreList.map(_._2).sum / scoreList.length + log(increMap.getOrElseUpdate(mid, 1)) - log(descreMap.getOrElseUpdate(mid, 1)) )
    }.toArray
  }
  // Find the similarity between two movies
  def getMoviesSimScore(mid1: Int, mid2: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
  : Double = {
    simMovies.get(mid1) match {
      case Some(sims) => sims.get(mid2) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }
  // base 10 log
  def log(m: Int): Double ={
    val N = 10
    math.log(m)/ math.log(N)
  }

  def saveDataToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit ={
    // define table connection
    val streamRecsCollection = ConnHelper.mongoClient
      .getDatabase(mongoConfig.db)
      .getCollection(MONGODB_STREAM_RECS_COLLECTION)

    // delete existing UID in the table
    streamRecsCollection.findOneAndDelete(Filters.eq("uid", uid))

    // store StreamRecs into the table
    val dataDoc = new Document()
      .append("uid", uid)
      .append("recs", streamRecs.map{
        case (mid, score) =>
          new Document().append("mid", mid).append("score", score)
      }.toList.asJava)
    streamRecsCollection.insertOne(dataDoc)

  }
}
