package org.bing.spark.rdd.standard.transformation

import org.apache.spark.{SparkConf, SparkContext}

object zip {
  def main(args:Array[String]):Unit={
    val conf=new SparkConf().setAppName("ZipTest").setMaster("local")
    val sc=new SparkContext(conf)

    val rdd1=sc.makeRDD(1 to 5,2)
    val rdd2=sc.makeRDD(Seq("A","B","C","D","E"),2)
    val rdd=rdd1.zip(rdd2)
    rdd.collect.foreach(println)
  }
}
