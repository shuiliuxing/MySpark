package org.bing.spark.rdd.standard.transformation

import org.apache.spark.{SparkConf, SparkContext}

object cogroup {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Cogroup Test").setMaster("local")
    val sc=new SparkContext(conf)

    cogroup1(sc)

  }

  def cogroup1(sc:SparkContext):Unit={
    val rdd1=sc.parallelize(Array((1,"a"),(2,"b"),(3,"c"),(4,"d")))
    val rdd2=sc.parallelize(Array((1,4),(2,5),(3,6)))
    val cogroupRdd=rdd1.cogroup(rdd2)
    cogroupRdd.foreach(println)
  }
}
