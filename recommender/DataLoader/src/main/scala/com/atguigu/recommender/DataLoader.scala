package com.atguigu.recommender

import java.net.InetAddress

import com.mongodb.casbah.Imports.{MongoClient, MongoClientURI, MongoDBObject}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.apache.spark.sql.functions._

//游戏数据集
//id 名字 描述 开发商 销售商 类型
case class Game(mid: Int,name:String,descri:String,timelong:String,Developers:String,genres:String)

//排名数据集
//用户id 电影id 评分 时间戳
case class Rating(uid:Int,mid:Int,score:Double,timestamp:Int)

//标签类
case class Tag(uid:Int,mid:Int,tag:String,timestamp:Int)

//mongo配置
/**
 *
 * @param uri MongoDB连接
 * @param db  MongoDB数据库
 */
case class MongoConfig(uri:String,db:String)

//ElasticSearch配置
/**
 *
 * @param httpHosts       http主机列表，逗号分隔
 * @param transportHosts  transport主机列表
 * @param index            需要操作的索引
 * @param clustername      集群名称，默认elasticsearch
 */
case class ESConfig(httpHosts:String,transportHosts:String,index:String,clustername:String)

object DataLoader {
  val GAME_DATA_PATH="D:\\works\\javaproject\\steamRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\games.csv"
  val RATING_DATA_PATH="D:\\works\\javaproject\\steamRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH="D:\\works\\javaproject\\steamRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"
  val MONGODB_GAME_COLLECTION="Game"
  val MONGODB_RATING_COLLECTION="Rating"
  val MONGODB_TAG_COLLECTION="Tag"
  val ES_GAME_INDEX="Game"

  def main(args: Array[String]): Unit = {
    //配置参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://node1:27017/gamerecommender",
      "mongo.db" -> "gamerecommender",
      "es.httpHosts" -> "node3:9200",
      "es.transportHosts" -> "node3:9300",
      "es.index" -> "gamerecommender",
      "es.cluster.name" -> "es-cluster"
    )

    val sparkConf: SparkConf = new SparkConf().setAppName("DataLoader").setMaster(config("spark.cores"))
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._

    //将game.csv数据加载进来
    val gameRDD = sparkSession.sparkContext.textFile(GAME_DATA_PATH)

    //将gameRDD转换为DataFrame
    val gameDF: DataFrame = gameRDD.map(item => {
      val attr = item.split("\\^")
      //使用样例类
      Game(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim, attr(5).trim)
    }).toDF()

//    val frame: DataFrame = gameDF.select(col("mid"),col("genres"))
////    println(frame.show())
//    val frame1 = frame.select(explode(split(col("genres"), "\\|")).as("genre"))
//    println(frame1.show(frame1.count().toInt,false))
//
//    val value: Dataset[Row] = frame1.dropDuplicates("genre")
//    val frame2 = value.toDF()
//    println(frame2.show(frame2.count().toInt,false))
//    frame2
//      .coalesce(1)
//      .write
//      .mode(SaveMode.Overwrite)
//      .option("header","false")
//      .csv("E:\\java_all_workerspace\\BIG_DATA\\steamRecommendSystem\\recommender\\DataLoader\\src\\main\\scala\\com\\atguigu\\recommender\\genres.csv")


    //    println(gameDF.show())
//    println(gameDF.select("genres").show(numRows =gameDF.count().toInt ,truncate = false))
//    val frame: DataFrame = gameDF.select("genres")
//    frame.show(frame.count(),false)
//    val frame: DataFrame = gameDF.select("genres")
//    val gamerdd: RDD[Row] = frame.rdd







    val ratingRDD = sparkSession.sparkContext.textFile(RATING_DATA_PATH)

    //将rating.csv转换为DataFrame
    val ratingDF: DataFrame = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

//    println(ratingDF.show())

    val tagRDD = sparkSession.sparkContext.textFile(TAG_DATA_PATH)

    val tagDF = tagRDD.map(item => {
      val attr = item.split(",")
      Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
    }).toDF()

//    println(tagDF)


    //数据预处理，把game对应的tag信息添加进去，加一页tag1|tag2
    implicit val mongoConfig: MongoConfig =MongoConfig(config("mongo.uri"),config("mongo.db"))

    //将数据保存到mongodb中
//    storeDataInMongoDB(gameDF, ratingDF, tagDF)


    val newTag: DataFrame = tagDF.groupBy($"mid")
      .agg(concat_ws("|", collect_set($"tag")).as("tags"))
      .select("mid", "tags")

    println(newTag.show(truncate = true))
//    需要将处理后的Tag数据，和game数据融合，产生新的game数据
    val gameWithTagDF: DataFrame = gameDF.join(newTag, Seq("mid"), "left")

//    声明一个ES配置的隐式参数
    implicit val eSConfig: ESConfig = ESConfig(config("es.httpHosts"),
      config("es.transportHosts"),
      config("es.index"),
      config("es.cluster.name"))

//    需要将新的movie数据保存到ES中
    storeDataInES(gameWithTagDF)

    sparkSession.close()

  }

  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, TagDF: DataFrame)(implicit mongoConfig: MongoConfig):Unit={

    //新建一个到mongodb的连接
    val mongoClient: MongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    //如果mongodb中有对应的数据库，那么应该删除
    mongoClient(mongoConfig.db)(MONGODB_GAME_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    //将当前数据写入到mongodb
    movieDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_GAME_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    TagDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对数据表建索引
    mongoClient(mongoConfig.db)(MONGODB_GAME_COLLECTION).createIndex(MongoDBObject("mid"->1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid"->1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid"->1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid"->1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid"->1))

    //关闭mongodb的连接
    mongoClient.close()

  }

  def storeDataInES(movieDF: DataFrame)(implicit eSConfig: ESConfig):Unit={
    //新建一个配置
    val settings = Settings.builder()
      .put("cluster.name", eSConfig.clustername).build()

    //新建一个ES的客户端
    val esclient: PreBuiltTransportClient = new PreBuiltTransportClient(settings)

    //需要将TransportHosts添加到esClient中
    val REGEX_HOST_PORT="(.+):(\\d+)".r

    eSConfig.transportHosts.split(",").foreach{
      case REGEX_HOST_PORT(host:String,port:String)=>{
        esclient.addTransportAddress(new
            InetSocketTransportAddress(InetAddress.getByName(host),port.toInt))
      }
    }

    //需要清除掉es中遗留的数据
    if(esclient.admin().indices().exists(new
        IndicesExistsRequest(eSConfig.index)).actionGet().isExists){
      esclient.admin().indices().delete(new DeleteIndexRequest(eSConfig.index))
    }

    esclient.admin().indices().create(new CreateIndexRequest(eSConfig.index))

    //将数据写入到es中
    movieDF.write
      .option("es.nodes",eSConfig.httpHosts)
      .option("es.http.timeout","100m")
      .option("es.mapping.id","mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(eSConfig.index+"/"+ES_GAME_INDEX)

  }
}
