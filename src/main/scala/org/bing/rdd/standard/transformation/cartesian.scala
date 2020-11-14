package org.bing.rdd.standard.transformation

import org.apache.spark.{SparkConf, SparkContext}

object cartesian {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Cartesian Test").setMaster("local")
    val sc=new SparkContext(conf)
    cartesian1(sc)
  }

  def cartesian1(sc:SparkContext):Unit={
    val rdd1=sc.parallelize(1 to 3)
    val rdd2=sc.parallelize(4 to 6)
    val cartesianRdd=rdd1.cartesian(rdd2)
    cartesianRdd.foreach(println)
  }
}
