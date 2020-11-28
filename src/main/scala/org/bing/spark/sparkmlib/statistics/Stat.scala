package org.bing.spark.sparkmlib.statistics

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Stat {
  def main(args: Array[String]): Unit = {
    val conf:SparkConf=new SparkConf().setAppName("Stat Test").setMaster("local")
    val sc:SparkContext=new SparkContext(conf)

    pcc(sc)
  }

  //均值、方差
  def mss(sc:SparkContext):Unit={
    val rddVec:RDD[Vector]=sc.textFile("E:\\data\\spark\\sparkmlib\\testSummary.txt")
      .map(_.split(" ").map(_.toDouble))
      .map(arr=>Vectors.dense(arr))
    val mss:MultivariateStatisticalSummary=Statistics.colStats(rddVec)
    println("均值："+mss.mean)
    println("标准差："+mss.variance)
    println("欧几里得距离："+mss.normL2)
    println("曼哈顿距离："+mss.normL1)
  }

  //皮尔逊相关系数（2组系数的相关性）
  def pcc(sc:SparkContext):Unit={
    val rddX:RDD[Double]=sc.textFile("E:\\data\\spark\\sparkmlib\\testCorrectX.txt")
               .flatMap(_.split(" ").map(_.toDouble))
    val rddY:RDD[Double]=sc.textFile("E:\\data\\spark\\sparkmlib\\testCorrectY.txt")
               .flatMap(_.split(" ").map(_.toDouble))
    val pcc:Double=Statistics.corr(rddX,rddY)
    println("皮尔逊相关系数："+pcc)
    val spm:Double=Statistics.corr(rddX,rddY,"spearman")
    println("斯皮尔曼系数："+spm)
  }


}
