package org.bing.spark.sparkmlib.recommend
//向量
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.recommendation.{ALS, Rating}
//向量集
import org.apache.spark.mllib.linalg.Vectors
//稀疏向量
import org.apache.spark.mllib.linalg.SparseVector
//稠密向量
import org.apache.spark.mllib.linalg.DenseVector
//实例
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
//矩阵
import org.apache.spark.mllib.linalg.{Matrix, Matrices}
//索引矩阵
import org.apache.spark.mllib.linalg.distributed.RowMatrix
//RDD
import org.apache.spark.rdd.RDD

object Test {
  def main(args: Array[String]) {
    // 构建Spark 对象
    //Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("HACK-AILX10").setMaster("local")
    val sc = new SparkContext(conf)
    // 读取样本数据
    val data = sc.textFile("E:\\data\\spark\\sparkmlib\\test.txt")
    val parsedData = data.map(_.split(",") match {
      case Array(user,item,rate) =>
        Rating(user.toInt,item.toInt,rate.toDouble)
    })

    //建立模型，并设置训练参数
    val model = ALS.train(parsedData,10,20,0.01)

    //预测结果
    val userProducts = parsedData.map{
      case Rating(user,product,rate) =>
        (user,product)
    }

    val predictions = model.predict(userProducts).map{
      case Rating(user,product,rate)=>
        ((user,product),rate)
    }
    println("（用户ID，物品ID），（评分，预测）")
    val ratesAndPreds = parsedData.map{
      case Rating(user,product,rate)=>
        ((user,product),rate)
    }.join(predictions)
    ratesAndPreds.collect().foreach(println(_))

    //误差计算
    val MSE = ratesAndPreds.map{
      case((user,product),(r1,r2))=>
        val err = (r1-r2)
        err*err
    }.mean()
    println("误差：" + MSE)

  }
}
