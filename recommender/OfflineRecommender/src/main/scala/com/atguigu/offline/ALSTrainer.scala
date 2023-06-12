//package com.atguigu.offline
//
//import breeze.numerics.sqrt
//import com.atguigu.offline.OfflineRecommender.MONGODB_RATING_COLLECTION
//import org.apache.spark.SparkConf
//import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SparkSession
//
//object ALSTrainer {
//  def main(args: Array[String]): Unit = {
//    val config = Map(
//      "spark.cores" -> "local[*]",
//      "mongo.uri" -> "mongodb://node1:27017/gamerecommender",
//      "mongo.db" -> "gamerecommender"
//    )
//    val sparkconf: SparkConf = new SparkConf().setAppName("ALSTrainer").setMaster(config("spark.cores"))
//    val sparkSession = SparkSession.builder().config(sparkconf).getOrCreate()
//
//    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
//
//    import sparkSession.implicits._
//
//    //加载数据
//    val ratingRDD = sparkSession
//      .read
//      .option("uri", mongoConfig.uri)
//      .option("collection", MONGODB_RATING_COLLECTION)
//      .format("com.mongodb.spark.sql")
//      .load()
//      .as[GameRating]
//      .rdd
//      .map(rating =>
//        Rating(rating.uid, rating.mid, rating.score))
//      .cache()
//
//    val spilts: Array[RDD[Rating]] = ratingRDD.randomSplit(Array(0.8, 0.2))
//
//    val trainingRDD: RDD[Rating] = spilts(0)
//    val testingRDD: RDD[Rating] = spilts(1)
//
//    adjustALSParams(trainingRDD,testingRDD)
//
//    sparkSession.close()
//  }
//
//  def adjustALSParams(trainData:RDD[Rating],testData:RDD[Rating]):Unit={
//    val result: Array[(Int, Double, Double)] = for (rank <- Array(100, 200, 250); lambda <- Array(1, 0.1, 0.01, 0.001))
//      yield {
//        val model: MatrixFactorizationModel = ALS.train(trainData, rank, 5, lambda)
//        val rmse: Double = getRMES(model, testData)
//        (rank, lambda, rmse)
//      }
//    print(result.minBy(_._3))
//  }
//
//
//  def getRMES(model:MatrixFactorizationModel,data:RDD[Rating]):Double={
//    val userGames = data.map(item => (item.user, item.product))
//    val predictRating = model.predict(userGames)
//    val real = data.map(item => ((item.user, item.product), item.rating))
//    val predict = predictRating.map(item => ((item.user, item.product), item.rating))
//
//    //计算RMSE
//    sqrt(
//      real.join(predict).map{case ((uid,mid),(real,pre))=>
//      val err: Double = real - pre
//        err *err
//      }.mean()
//    )
//  }
//
//}
////(200, 0.1, 1.1922584598027584)
////200, 1.0, 1.6619954214883212