package com.Jeff.offline

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSTrainer {
  val MONGODB_RATING_COLLECTION = "Rating"
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
    println("Success Config")
    // 加载评分数据
    val ratingRDD = spark.read
      .option("spark.mongodb.read.connection.uri", mongoConfig.uri)
      .option("database", mongoConfig.db)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("mongodb")
      .load()
      .as[movieRating]
      .rdd
      .map(
        rating => Rating(rating.uid, rating.mid, rating.score)
      ) // remove the time tick
      .cache()
    println("Success READ")
    // split dataset, Training and testing
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingRDD = splits(0)
    val testRDD = splits(1)
    println("Read to adjust PARAM")
    // model parameter selection, find optim parameter
    adjustALSParam(trainingRDD, testRDD)

    spark.close()
  }

  def adjustALSParam(trainData: RDD[Rating], testData: RDD[Rating]): Unit ={
    val result = for (
        rank <- Array(20, 50, 100,200,300);
        lambda <- Array(0.001, 0.01, 0.1))
      yield{
        println("Start")
        val model = ALS.train(trainData, rank, 10, lambda)
        val rmse = getRMSE( model, testData )
        println(s"rank: $rank lambda: $lambda rmse: $rmse")
        ( rank, lambda, rmse )

      }

    // print the optim parameter
    println(result.minBy(_._3))

  }

  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    // predict
    val userProducts = data.map(item => (item.user, item.product))
    val predictRating = model.predict(userProducts)

    // based on uid, mid as foregin key
    val observed = data.map( item => ((item.user, item.product), item.rating))
    val predict = predictRating.map( item => ((item.user, item.product), item.rating))

    sqrt(
      // inner join -> (uid, mid), (actual, predict)
      //TODO 为什么没有groupBy的操作
      observed.join(predict).map{
        case ((_, _), (actual, pre)) =>
          val err = actual - pre
          err * err
      }.mean()
    )


  }
}
