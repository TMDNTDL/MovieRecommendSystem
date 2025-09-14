package com.Jeff.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix


// based on score data model LFM, we only need rating
case class movieRating(uid: Int, mid:Int, score: Double, timestamp: Int)

// Recommendation case Object
case class Recommendation (mid: Int, score: Double)

// define User recommendation case class
case class UserRecs( uid: Int, recs: Seq[Recommendation])

// based on LFM movie characteristic vector, movie similarity list
case class MovieRecs( mid: Int, Recs: Seq[Recommendation])
case class MongoConfig(uri:String, db:String)

object ALSRecommender {
  val MONGODB_RATING_COLLECTION = "Rating"
  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"

  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ALSRecommender")

    // create a spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // Load Rating Data
    val ratingRDD = spark.read
      .option("spark.mongodb.read.connection.uri", mongoConfig.uri)
      .option("database", mongoConfig.db)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("mongodb")
      .load()
      .as[movieRating]
      .rdd
      .map(
        rating => (rating.uid, rating.mid, rating.score)
      ) // remove the time tick
      .cache()

    println(s"✅ Loaded rating data: ${ratingRDD.count()} rows")

    // ALS model depend on history score
    // loading uid, mid from rating, and remove duplication
    val userRDD = ratingRDD.map(_._1).distinct()
    val movieRDD = ratingRDD.map(_._2).distinct()
    println(s"✅ Distinct users: ${userRDD.count()}, movies: ${movieRDD.count()}")
    // Training ALS Model
    val trainData = ratingRDD.map( x => Rating(x._1, x._2, x._3))

    val (rank, iterations, lambda) = (100,10,0.1)
    // rank is the number of features
    val model = ALS.train(trainData, rank, iterations, lambda)
    println(s"✅ ALS model trained: rank=$rank, iterations=$iterations, lambda=$lambda")

    // Based on user char matrix and movie char matrix, calculate the predict score,get user recommend list
    // calculate user and movie cartesian, return an empty score matrix
    val userMovies = userRDD.cartesian(movieRDD)

    // predict, return RDD[Rating] (user,product, rating)
    val preRatings = model.predict(userMovies)
    println(s"✅ Predictions done: ${preRatings.count()} entries")


    val userRecs = preRatings
      .filter(_.rating > 0)
      .map(rating => (rating.user, (rating.product, rating.rating))) // turn last two item into tuple
      .groupByKey()
      .map{
        case (uid, recs) => UserRecs(uid, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION)
          .map(
            x => Recommendation(x._1, x._2)
          ))
      }
      .toDF()
    println(s"✅ User recommendations generated: ${userRecs.count()} users")

    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("spark.mongodb.database", mongoConfig.db)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("mongodb")
      .save()
    println("💾 UserRecs saved to MongoDB")

    // Movie similarity calculation
    val movieFeatures = model.productFeatures.map{
      case (mid, features) => (mid, new DoubleMatrix(features))
    }
    println(s"✅ Movie feature vectors generated: ${movieFeatures.count()} movies")

    // 对所有电影两两计算他们的相似度，先做笛卡尔积
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter{
        case (a, b) => a._1 != b._1
      }
      .map{
        case (a,b) => {
          val simScore = this.consinSim(a._2, b._2)
          ( a._1, ( b._1, simScore) )
        }
      }
      .filter(_._2._2 > 0.6) // filter out score more than 0.6
      .groupByKey()
      .map{
        case (mid, items) => MovieRecs( mid, items.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()
    println(s"✅ Movie similarity recommendations generated: ${movieRecs.count()} movies")

    movieRecs.write
      .option("uri", mongoConfig.uri)
      .option("spark.mongodb.database", mongoConfig.db)
      .option("collection", MOVIE_RECS)
      .mode("overwrite")
      .format("mongodb")
      .save()
    println("💾 MovieRecs saved to MongoDB")
    spark.stop()
  }
  // consine distance
  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix):Double ={
    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
  }

}


