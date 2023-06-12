package com.atguigu.statistics

import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

//游戏数据集
//id 名字 描述 开发商 销售商 类型
case class Game(mid: Int,name:String,descri:String,timelong:String,Developers:String, genres:String)

//排名数据集
//用户id 游戏id 评分 时间戳
case class Rating(uid:Int,mid:Int,score:Double,timestamp:Int)

//标签类
case class Tag(uid:Int,mid:Int,tag:String,timestamp:Int)

case class MongoConfig(uri:String, db:String)

// 定义一个基准推荐对象
case class Recommendation( mid: Int, score: Double )

// 定义游戏类别top10推荐对象
case class GenresRecommendation( genres: String, recs: Seq[Recommendation] )

object StatisticsRecommender {
  // 定义表名
  val MONGODB_GAME_COLLECTION = "Game"
  val MONGODB_RATING_COLLECTION = "Rating"

  //统计的表的名称
  val RATE_MORE_GAMES = "RateMoregames"
  val RATE_MORE_RECENTLY_GAMES = "RateMoreRecentlygames"
  val AVERAGE_GAMES = "Averagegames"
  val GENRES_TOP_GAMES = "GenresTopgames"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://node1:27017/gamerecommender",
      "mongo.db" -> "gamerecommender"
    )

    // 创建一个sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommeder")

    // 创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    //将数据加载进去
    val ratingDF = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val gameDF = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_GAME_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Game]
      .toDF()

    //创建一张名叫ratings的表
    ratingDF.createOrReplaceTempView("ratings")

    //统计所有历史数据中每个游戏的评分数--历史热门游戏统计
    //数据结构 -》 mid,count
    val rateMoreGameDF = spark.sql(
      "select mid,count(mid) as count from ratings group by mid"
    )

//    println(rateMoreGameDF.show())

    rateMoreGameDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",RATE_MORE_GAMES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //统计以月为单位拟每个电影的评分数---最近热门电影统计
    //数据结构--》 mid,count,time

    val simpleDateFormat = new SimpleDateFormat("yyyyMM")

    spark.udf.register("changeDate",(x:Int)=>simpleDateFormat.format(new Date(x*1000L)).toInt)

    val ratingofYearMonth = spark.sql(
      "select mid,score,changeDate(timestamp) as yearmonth from ratings"
    )

//    println(ratingofYearMonth.show())

    ratingofYearMonth.createOrReplaceTempView("ratingOfMonth")

    val rateMoreRecentlyGames = spark.sql(
      "select mid,count(mid) as count,yearmonth from ratingOfMonth group by yearmonth,mid"
    )

//    println(rateMoreRecentlyGames.show())
    rateMoreRecentlyGames
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",RATE_MORE_RECENTLY_GAMES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //统计每个游戏的平均分
    val averageGamesDF = spark.sql(
      "select mid,avg(score) as avg from ratings group by mid"
    )

//    println(averageGamesDF.show())
    averageGamesDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",AVERAGE_GAMES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 统计每种电影类型评分最高的10个电影--每个类别优质电影统计

    //Seq是指连接的字段
    val gameWithScore: DataFrame = gameDF.join(averageGamesDF, Seq("mid"))

    println(gameWithScore.show(truncate = false))

    val genres =
      List("Action","Adventure","Free to Play","Indie","Simulation","Early Access","RPG",
      "Massively Multiplayer","Casual","Strategy","Sports","Racing")

    val genresRDD: RDD[String] = spark.sparkContext.makeRDD(genres)

    val genrenTopGames: DataFrame = genresRDD.cartesian(gameWithScore.rdd)
      .filter {
        case (genres, row) =>
          row.getAs[String]("genres").toLowerCase.contains(genres.toLowerCase)
      }
      .map {
        case (genres, row) =>
          (genres, (row.getAs[Int]("mid"), row.getAs[Double]("avg")))
      }.groupByKey()
      .map {
        case (genres, items) =>
          GenresRecommendation(genres, items.toList.sortWith(_._2 > _._2).take(10).map(
            item => Recommendation(item._1, item._2)
          ))
      }.toDF()

    // 输出数据到 MongoDB
    genrenTopGames
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",GENRES_TOP_GAMES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    spark.stop()

  }
}