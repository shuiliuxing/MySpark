package org.bing.rdd.standard.transformation

import org.apache.spark.{SparkConf, SparkContext}

object repartition {
  def main(args:Array[String]):Unit={
    val conf=new SparkConf().setAppName("Repartition Test").setMaster("local")
    val sc=new SparkContext(conf)
    repartition1(sc)
  }

  def repartition1(sc:SparkContext):Unit={
    val rdd=sc.textFile("E:\\data\\spark\\rdd\\test\\read\\ml-25m\\genome-scores.csv")
    println(rdd.getNumPartitions)
    rdd.repartition(1).saveAsTextFile("E:\\data\\spark\\rdd\\test\\read\\ml-25m\\genome-scores")
  }

}
