package org.bing.spark.rdd.standard.action

import org.apache.spark.{SparkConf, SparkContext}

object foreach {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Count Test").setMaster("local")
    val sc=new SparkContext(conf)

    foreach1(sc)
  }

  def foreach1(sc:SparkContext):Unit={
    val rdd=sc.parallelize(Array(("A","1"),("B",2),("C",3),("A",5)))
    rdd.foreach(println)
  }

}
