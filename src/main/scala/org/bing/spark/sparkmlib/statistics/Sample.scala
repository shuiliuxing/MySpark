package org.bing.spark.sparkmlib.statistics

import org.apache.spark.mllib.linalg.{Matrices, Vectors}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Sample {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Sample Test").setMaster("local")
    val sc=new SparkContext(conf)

    random(sc)
  }

  //分层抽样
  def levelSample(sc:SparkContext):Unit={
    val rdd:RDD[(String,String)]=sc.makeRDD(Array(
           ("female","Lily"),
           ("female","Lucy"),
           ("female","Emily"),
           ("female","Kate"),
           ("female","Alice"),
           ("male","Tom"),
           ("male","Roy"),
           ("male","David"),
           ("male","Frank"),
           ("male","Jack")
    ))
    val fact:Map[String,Double]=Map("female"->0.6, "male"-> 0.4)
    val appSample:RDD[(String,String)]=rdd.sampleByKey(false, fact, 1)
    appSample.foreach(println)
  }

  //假设检验
  def chis(sc:SparkContext):Unit={
    val vd=Vectors.dense(1,2,3,4,5)   //创建数据集
    val vdResult=Statistics.chiSqTest(vd)   //读取数据
    println(vdResult)
    println()

    val mx=Matrices.dense(3,2,Array(1,3,5,2,4,6))
    val mxResult=Statistics.chiSqTest(mx)     //读取矩阵数据
    println(mxResult)
  }

  //随机数
  def random(sc:SparkContext):Unit={
    val rdd=RandomRDDs.normalRDD(sc,20) //创建20个随机数
    rdd.foreach(println)
  }
}
