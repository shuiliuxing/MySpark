package org.bing.spark.rdd.apply

import org.apache.spark.{SparkConf, SparkContext}

object wordAnalyse {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("MaxLine Word").setMaster("local")
    val sc=new SparkContext(conf)

    wordAnalyse2(sc)
  }

  def wordAnalyse1(sc:SparkContext):Unit={
    val rdd=sc.textFile("E:\\data\\spark\\rdd\\test\\read\\word.log")
    val flatMapRdd=rdd.flatMap(line=>line.replace("."," ").replace(";"," ").split(" ")).map(x=>(x,1)).reduceByKey(_+_)
    flatMapRdd.foreach(println)
  }

  def wordAnalyse2(sc:SparkContext):Unit={
    val rdd=sc.textFile("E:\\data\\spark\\rdd\\test\\read\\word.log")
    val flatMapRdd=rdd.flatMap(line=>line.replace("."," ").replace(";"," ")
                      .split(" "))
                      .map(x=>(x,1))
                      .reduceByKey(_+_)
                      .map(x=>(x._2,x._1))
                      .sortByKey()
                      .map(x=>(x._2,x._1))
    flatMapRdd.foreach(println)
  }

}



