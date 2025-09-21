package com.Jeff

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix
import org.apache.spark.ml.feature.{CountVectorizer, HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
//Dataset is movie content
case class Movie(mid: Int, name:String, descri:String, timelong:String, issue:String,
                 shoot:String, language: String, genres:String, actors: String, directors:String)
case class MongoConfig(uri:String, db:String)

case class Recommendation(mid: Int, score: Double)

// based movie content extract characteristics
case class MovieRecs(mid: Int, recs: Seq[Recommendation])
object ContentRecommender {
  // 定义表名和常量
  val MONGODB_MOVIE_COLLECTION = "Movie"

  val CONTENT_MOVIE_RECS  = "ContentMovieRecs"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ContentRecommender")

    // spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    //Load data
    // turn into dataframe
    val movieTagsDF = spark.read
      .option("spark.mongodb.read.connection.uri",mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .option("database", mongoConfig.db)
      .format("mongodb")
      .load()
      .as[Movie]
      .map(
        // default split by space
        x => (x.mid, x.name, x.genres.map(c=> if (c == '|') ' ' else c))
      ) // we want mid, name, genres only for extract features
      .toDF("mid", "name", "genres")
      .cache()

    //using TF-IDF extract movie Features

    // create a tokenizer
    val tokenizer = new Tokenizer().setInputCol("genres").setOutputCol("words")

    // use tokenizer to convert original data
    val wordsData = tokenizer.transform(movieTagsDF)

    // import HashingTF Tools, turn word serialize into frequency
    // setting the numFeature higher to avoid hashing collidion
    // 变成一个稀疏向量的表达式， 3-tuple
    val cv = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
    val cvModel = cv.fit(wordsData)

    //val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(150)
    val featurized = cvModel.transform(wordsData)

    // import IDF Tools
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    // get the inverse data frequency
    val idfModel = idf.fit(featurized) // just IDF vector 1x50 in this case

    // pre-process all data, and get tf-idf as new vector
    // multiple each row non-zero index i with IDF(i) to return TF-IDF
    val rescaleData = idfModel.transform(featurized)

    rescaleData.show(truncate = false)

    val movieFeatures = rescaleData.map(
      row => (row.getAs[Int]("mid"), row.getAs[SparseVector]("features").toArray)

    )
      .rdd
      .map(
        x => (x._1, new DoubleMatrix(x._2))
      )

    //movieFeatures.collect().foreach(println)
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
      .option("collection", CONTENT_MOVIE_RECS)
      .mode("overwrite")
      .format("mongodb")
      .save()
    println("💾 MovieRecs saved to MongoDB")
    spark.stop()
  }

  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix):Double ={
    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
  }
}
