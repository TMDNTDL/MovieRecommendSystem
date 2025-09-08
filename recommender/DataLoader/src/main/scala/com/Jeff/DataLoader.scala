package com.Jeff

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch.indices.{CreateIndexRequest, DeleteIndexRequest, ExistsRequest}
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.rest_client.RestClientTransport
import com.mongodb.client.MongoClients
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mongodb.client.model.Indexes
import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.elasticsearch.client.RestClient



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

    // 在main方法中设置ES配置
    spark.conf.set("es.nodes", "localhost")
    spark.conf.set("es.port", "9200")
    spark.conf.set("es.net.http.auth.user", "elastic")
    spark.conf.set("es.net.http.auth.pass", "#Yuegong123")
    spark.conf.set("es.net.ssl", "true")
//    spark.conf.set("es.nodes.wan.only", "true")
    spark.conf.set("es.net.ssl.truststore.location", "C:/Develop/ElasticSearchCertificate/http_ca.crt")
    spark.conf.set("es.net.ssl.truststore.type", "PEM")
    spark.conf.set("es.net.ssl.truststore.pass", "")
    spark.conf.set("es.mapping.id", "mid")


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
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("zdb"))

    // 将数据保存到MongoDB
    storeDataInMongoDB(movieDF, ratingDF, tagDF)

    // Data Pre-processing, add movie with its corresponding tag info, tag1|tag2|tag3..
    import org.apache.spark.sql.functions._

    /**
     * mid, tags
     *
     * tags: tag1|tag2|tag3...
     */
    val newTag = tagDF.groupBy($"mid")
      .agg( concat_ws("|", collect_set($"tag")).as("tags"))
      .select("mid", "tags")

    // tags join with movie, combine them together, 左外链接
    val movieWithTagsDF = movieDF.join(newTag, Seq("mid"), "left")

    implicit val esConfig = ESConfig(config("es.httpHosts"),config("es.transportHosts"), config("es.index"), config("es.cluster.name"))

    // 保存数据到ES
    storeDataInES(movieWithTagsDF)

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
//  def storeDataInES(MovieDF: DataFrame)(implicit eSConfig: ESConfig): Unit = {
//    val hostName = "yg-node-1"
//    val certFile = new File("C:/Develop/ElasticSearchCertificate/http_ca.crt")
//
//    // 1. 修复Java客户端连接问题
//    val sslContext: SSLContext = TransportUtils.sslContextFromHttpCaCrt(certFile)
//
//    val credsProv = new BasicCredentialsProvider()
//    credsProv.setCredentials(
//      AuthScope.ANY,
//      new UsernamePasswordCredentials("elastic", "#Yuegong123")
//    )
//
//    // 禁用主机名验证
//    import org.apache.http.conn.ssl.NoopHostnameVerifier
//    val hostnameVerifier = new NoopHostnameVerifier()
//
//    val restClient = RestClient.builder(
//        new HttpHost(hostName, 9200, "https")
//      )
//      .setHttpClientConfigCallback((httpClientBuilder: HttpAsyncClientBuilder) => {
//        httpClientBuilder
//          .setSSLContext(sslContext)
//          .setSSLHostnameVerifier(hostnameVerifier) // 添加这行
//          .setDefaultCredentialsProvider(credsProv)
//      })
//      .build()
//
//    val transport = new RestClientTransport(restClient, new JacksonJsonpMapper())
//    val esClient = new ElasticsearchClient(transport)
//
//    val index = eSConfig.index.toLowerCase()
//
//    try {
//      println("测试Elasticsearch连接...")
//
//      // 先测试连接
//      val info = esClient.info()
//      println(s"✓ 连接到Elasticsearch集群: ${info.clusterName()}")
//      println(s"✓ 版本: ${info.version().number()}")
//
//      val req: ExistsRequest = new ExistsRequest.Builder().index(index).build()
//      val resp: BooleanResponse = esClient.indices().exists(req)
//      val exists: Boolean = resp.value()
//
//      if (exists) {
//        println(s"索引 $index 已存在，正在删除...")
//        val delReq = new DeleteIndexRequest.Builder().index(index).build()
//        esClient.indices().delete(delReq)
//        println(s"✓ 已删除索引: $index")
//      }
//
//      // 创建索引
//      println(s"创建索引: $index")
//      val createReq = new CreateIndexRequest.Builder().index(index).build()
//      esClient.indices().create(createReq)
//      println(s"✓ 已创建索引: $index")
//
//    } catch {
//      case e: Exception =>
//        println(s"ES客户端操作失败: ${e.getMessage}")
//        e.printStackTrace()
//        return // 如果Java客户端失败，直接返回
//    } finally {
//      transport.close()
//      restClient.close()
//    }
//
//    // 2. 修复Spark写入问题
//    try {
//      println("开始使用Spark写入数据到Elasticsearch...")
//
//      // 最简单的写法 - 使用saveToEs方法
//      import org.elasticsearch.spark.sql._
//
//      MovieDF.saveToEs(s"$index/ES_MOVIE_INDEX")
//
//      println("✓ 数据已成功写入Elasticsearch")
//
//    } catch {
//      case e: Exception =>
//        println(s"Spark写入ES失败: ${e.getMessage}")
//        e.printStackTrace()
//
//        // 如果HTTPS失败，尝试HTTP
//        try {
//          println("尝试使用HTTP协议...")
//          MovieDF.saveToEs(s"$index/_doc", Map(
//            "es.nodes" -> "172.18.41.215",
//            "es.port" -> "9200",
//            "es.net.http.auth.user" -> "elastic",
//            "es.net.http.auth.pass" -> "#Yuegong123",
//            "es.net.ssl" -> "false", // 使用HTTP
//            "es.nodes.wan.only" -> "true",
//            "es.mapping.id" -> "mid"
//          ))
//
//          println("✓ 使用HTTP协议成功写入数据")
//        } catch {
//          case e2: Exception =>
//            println(s"HTTP方案也失败: ${e2.getMessage}")
//            e2.printStackTrace()
//        }
//    }
//  }
def storeDataInES(MovieDF: DataFrame)(implicit eSConfig: ESConfig): Unit = {
  val hostName = "yg-node-1"

  // Java客户端 - 使用HTTP
  val credsProv = new BasicCredentialsProvider()
  credsProv.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "#Yuegong123"))

  val restClient = RestClient.builder(new HttpHost(hostName, 9200, "http")) // 改为http
    .setHttpClientConfigCallback(httpClientBuilder =>
      httpClientBuilder.setDefaultCredentialsProvider(credsProv)
    ).build()

  val transport = new RestClientTransport(restClient, new JacksonJsonpMapper())
  val esClient = new ElasticsearchClient(transport)

  val index = eSConfig.index.toLowerCase()

  try {
    println("测试Elasticsearch连接...")
    val info = esClient.info()
    println(s"连接到ES集群: ${info.clusterName()}, 版本: ${info.version().number()}")

    val req: ExistsRequest = new ExistsRequest.Builder().index(index).build()
    val exists: Boolean = esClient.indices().exists(req).value()

    if (exists) {
      esClient.indices().delete(new DeleteIndexRequest.Builder().index(index).build())
      println(s"已删除索引: $index")
    }

    esClient.indices().create(new CreateIndexRequest.Builder().index(index).build())
    println(s"已创建索引: $index")

  } finally {
    transport.close()
    restClient.close()
  }

  // Spark写入ES - 使用HTTP
  try {
    println("开始Spark写入...")
    import org.elasticsearch.spark.sql._

    MovieDF.saveToEs(index, Map(
      "es.nodes" -> hostName,
      "es.port" -> "9200",
      "es.net.http.auth.user" -> "elastic",
      "es.net.http.auth.pass" -> "#Yuegong123",
      "es.net.ssl" -> "false",
      "es.mapping.id" -> "mid"
    ))

    println("Spark写入成功!")

  } catch {
    case e: Exception =>
      println(s"Spark写入失败: ${e.getMessage}")
      e.printStackTrace()
  }
}


}
