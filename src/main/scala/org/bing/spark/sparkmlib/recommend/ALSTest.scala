package org.bing.spark.sparkmlib.recommend

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ALSTest {
  
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("ALS Test").setMaster("local")
    val sc=new SparkContext(conf)

  }

  def als(sc:SparkContext): Unit ={
    //用户对商品评分
    val rdd=sc.textFile("E:\\data\\spark\\sparkmlib\\ul.txt")
    val rating:RDD[Rating]=rdd.map(_.split(" ")match{
      case Array(user, prod, rate)=>{
        Rating(user.toInt, prod.toInt, rate.toInt)
      }
    })
    //Rating，隐藏因子 ，迭代次数
    val model:MatrixFactorizationModel=ALS.train(rating,2,2,0.01)   //进行模型训练
    val rs=model.recommendProducts(2,1)                 //为用户2推荐1个商品
    rs.foreach(println)
    
  }
}
