package com.Jeff

import com.mongodb.client.{MongoClients, MongoCollection, MongoDatabase}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mongodb.client.model.Indexes


/**
 * Movie DataSet Sample:
 * 260
 * Star Wars: Episode IV - A New Hope (1977)
 * Princess Leia is captured and held hostage by the evil Imperial forces in their effort to take over the galactic Empire. Venturesome Luke Skywalker and dashing captain Han Solo team together with the loveable robot duo R2-D2 and C-3PO to rescue the beautiful princess and restore peace and justice in the Empire.
 * 121 minutes
 * September 21, 2004
 * 1977
 * English
 * Action|Adventure|Sci-Fi Mark Hamill|Harrison Ford|Carrie Fisher|Peter Cushing|Alec Guinness|Anthony Daniels|Kenny Baker|Peter Mayhew|David Prowse|James Earl Jones|Phil Brown|Shelagh Fraser|Jack Purvis|Eddie Byrne|Denis Lawson|Garrick Hagon|Don Henderson|Leslie Schofield|Richard LeParmentier|Michael Leader|Alex McCrindle|Drewe Henley|Jack Klaff|William Hootkins|Angus MacInnes|Jeremy Sinden|Graham Ashley|David Ankrum|Mark Austin|Scott Beach|Lightning Bear|Jon Berg|Doug Beswick|Paul Blake|Janice Burchette|Ted Burnett|John Chapman|Gilda Cohen|Tim Condren|Barry Copping|Alfie Curtis|Robert Davies|Maria De Aragon|Robert A. Denham|Frazer Diamond|Peter Diamond|Warwick Diamond|Sadie Eden|Kim Falkinburg|Harry Fielder|Ted Gagliano|Salo Gardner|Steve Gawley|Barry Gnome|Rusty Goffe|Isaac Grand|Nelson Hall|Reg Harding|Alan Harris|Frank Henson|Christine Hewett|Arthur Howell|Tommy Ilsley|Joe Johnston|Annette Jones|Linda Jones|Joe Kaye|Colin Michael Kitchens|Melissa Kurtz|Tiffany L. Kurtz|Al Lampert|Anthony Lang|Laine Liska|Derek Lyons|Mahjoub|Alf Mangan|Rick McCallum|Grant McCune|Geoffrey Moon|Mandy Morton|Lorne Peterson|Marcus Powell|Shane Rimmer|Pam Rose|George Roubicek|Erica Simmons|Angela Staines|George Stock|Roy Straite|Peter Sturgeon|Peter Sumner|John Sylla|Tom Sylla|Malcolm Tierney|Phil Tippett|Burnell Tucker|Morgan Upton|Jerry Walter|Hal Wamsley|Larry Ward|Diana Sadley Way|Harold Weed|Bill Weston|Steve 'Spaz' Williams|Fred Wood|Colin Higgins|Mark Hamill|Harrison Ford|Carrie Fisher|Peter Cushing|Alec Guinness
 * George Lucas
 */

/**
 *
 * @param mid
 * @param name
 * @param descri
 * @param timelong
 * @param issue
 * @param shoot
 * @param language
 * @param genres
 * @param actors
 * @param directors
 */
case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)


/**
 * Rating dataset sample
 * 1,31,2.5,1260759144
 */
case class Rating(uid:Int, mid: Int, score: Double, timestamp: Int)

/**
 * Tag dataset sample
 * 15,1955,dentist,1193435061
 */
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)


// Encapulse MongoDB configuration into a class

/**
 *
 * @param uri MongoDB Connection URI
 * @param db MongoDB database
 */
case class MongoConfig(uri:String, db:String)

/**
 *
 * @param httpHosts http host list, separate by comma
 * @param transportHosts transport host list, cluster internal connection
 * @param index index
 * @param clustername clusterName
 */
case class ESConfig(httpHosts: String, transportHosts:String, index:String, clustername:String)

object DataLoader {
  // 定义常量
  val MOVIE_DATA_PATH = "C:\\Source\\Recommder\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH = "C:\\Source\\Recommder\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH= "C:\\Source\\Recommder\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"
  def main(args: Array[String]): Unit = {

    // Configuration parameter
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "localhost:9200",
      "es.transportHosts" -> "localhost:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "elasticsearch"
    )
    // 创建一个sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")

    // 创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()


    import spark.implicits._
    // 加载数据
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    val movieDF = movieRDD.map(
      item =>{
        val attr = item.split("\\^")
        Movie(attr(0).toInt, attr(1).trim, attr(2).trim,attr(3).trim,attr(4).trim,attr(5).trim,attr(6).trim,attr(7).trim,attr(8).trim,attr(9).trim)
      }
    ).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)

    val ratingDF = ratingRDD.map(
      item => {
        val attr = item.split(",")
        Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
      }
    ).toDF()

    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)
    val tagDF = tagRDD.map(
      item => {
        val attr = item.split(",")
        Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
      }
    ).toDF()

    // MongoDB Configuration
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 将数据保存到MongoDB
    storeDataInMongoDB(movieDF, ratingDF, tagDF)

    // 数据预处理



    // 保存数据到ES
    storeDataInES()

    spark.stop()
  }
  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    // create a mongoDB connection
    //mongoClient
    val client = MongoClients.create(mongoConfig.uri)
    val database = client.getDatabase(mongoConfig.db)

    // Delete existing DB, create new db
    val movieCollection = database.getCollection(MONGODB_MOVIE_COLLECTION)
    val ratingCollection = database.getCollection(MONGODB_RATING_COLLECTION)
    val tagCollection = database.getCollection(MONGODB_TAG_COLLECTION)

    movieCollection.drop()
    ratingCollection.drop()
    tagCollection.drop()

    // spark writing into mongoDB
    movieDF.write
      .option("uri", mongoConfig.uri)
      .option("spark.mongodb.database", mongoConfig.db)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("mongodb")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("spark.mongodb.database", mongoConfig.db)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("mongodb")
      .save()

    tagDF.write
      .option("uri", mongoConfig.uri)
      .option("spark.mongodb.database", mongoConfig.db)
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("mongodb")
      .save()

    movieCollection.createIndex(Indexes.ascending("mid"))
    ratingCollection.createIndex(Indexes.ascending("mid"))
    ratingCollection.createIndex(Indexes.ascending("uid"))
    tagCollection.createIndex(Indexes.ascending("mid"))
    tagCollection.createIndex(Indexes.ascending("uid"))

    // 关闭client
    client.close()
  }
  def storeDataInES(): Unit = {

  }



}
