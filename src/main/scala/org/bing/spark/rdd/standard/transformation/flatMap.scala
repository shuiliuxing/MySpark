package org.bing.spark.rdd.standard.transformation

import org.apache.spark.{SparkConf, SparkContext}

object flatMap {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("FlatMap Test").setMaster("local")
    val sc=new SparkContext(conf)

    //flatMap1(sc)
    flatMap2(sc)

  }

  //简单1
  def flatMap1(sc:SparkContext):Unit={
    val rdd=sc.parallelize(1 to 10,3)
    rdd.foreach(println)
  }

  //简单2
  def flatMap2(sc:SparkContext):Unit={
    val rdd=sc.parallelize(Array(("A",1),("B",2),("C",3)))
    rdd.flatMap(x=>(x._1+x._2)).foreach(println)
  }

  //简单3
}
