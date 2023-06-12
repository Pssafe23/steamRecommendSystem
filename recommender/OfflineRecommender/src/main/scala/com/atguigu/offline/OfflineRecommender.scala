package com.atguigu.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix


//游戏数据集
//id 名字 描述 开发商 销售商 类型
case class Game(mid: Int,name:String,descri:String,timelong:String,Developers:String,genres:String)

//排名数据集
//用户id 电影id 评分 时间戳
case class GameRating(uid:Int,mid:Int,score:Double,timestamp:Int)

case class MongoConfig(uri:String,db:String)

// 标准推荐对象，mid,score
case class Recommendation(mid: Int, score:Double)

// 定义基于预测评分的用户推荐列表
case class UserRecs(uid: Int, recs: Seq[Recommendation])

// 游戏相似度（游戏推荐）
case class GameRecs(mid: Int, recs: Seq[Recommendation])

object OfflineRecommender {

  // 定义常量
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_GAME_COLLECTION = "Game"
  // 推荐表的名称
  val USER_RECS = "UserRecs"
  val GAME_RECS = "GameRecs"

  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {

    // 定义配置
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://node1:27017/gamerecommender",
      "mongo.db" -> "gamerecommender"
    )
    val conf: SparkConf = new SparkConf().setAppName("OfflineRecommender").setMaster(config("spark.cores"))
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    import spark.implicits._

    //读取mongodb数据
    val ratingRDD: RDD[(Int, Int, Double)] = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[GameRating]
      .rdd
      .map(rating => (rating.uid, rating.mid, rating.score))
      .cache()

//    ratingRDD.collect().foreach(print(_))

    val userRDD: RDD[Int] = ratingRDD.map(_._1).distinct()

    userRDD.collect().foreach(print(_))

    val gameRDD: RDD[Int] = ratingRDD.map(_._2).distinct()

    val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))

    val (rank,iterations,lambda)=(200,1,1)
    //调用ALS算法训练隐语义模型
    val model: MatrixFactorizationModel = ALS.train(trainData, rank, iterations, lambda)
    //计算用户推荐矩阵
    val userGames: RDD[(Int, Int)] = userRDD.cartesian(gameRDD)
//    userGames.collect().foreach(print(_))
    //model已训练好，把id传进去就可以得到预测评分列表rdd(rating)(uid,mid,rating)
    val preRatings: RDD[Rating] = model.predict(userGames)
//        preRatings.collect().foreach(print(_))
    val userRecs: DataFrame = preRatings
      .filter(_.rating > 0)
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        case (uid, recs) => UserRecs(uid, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
      }.toDF()
//    println(userRecs.show(truncate = false))

    userRecs
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    val gameFeatures: RDD[(Int, DoubleMatrix)] = model.productFeatures.map {
      case (mid, features) =>
        (mid, new DoubleMatrix(features))
    }


    //计算笛卡尔积并过滤合并
    val gameRecs: DataFrame = gameFeatures.cartesian(gameFeatures)
      .filter { case (a, b) => a._1 != b._1 }
      .map {
        case (a, b) =>
          val simscore = this.consinSim(a._2, b._2)
          (a._1, (b._1, simscore))
      }.filter(_._2._2 > 0.6)
      .groupByKey()
      .map {
        case (mid, items) =>
          GameRecs(mid, items.toList.map(x => Recommendation(x._1, x._2)))
      }.toDF()

    gameRecs
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",GAME_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }
//计算两个电影之间的余弦相似度
  def consinSim(games1:DoubleMatrix,games2:DoubleMatrix):Double={
    games1.dot(games2) / (games1.norm2() * games2.norm2())
  }

}
