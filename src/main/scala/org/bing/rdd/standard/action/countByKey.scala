package org.bing.rdd.standard.action

import org.apache.spark.{SparkConf, SparkContext}

object countByKey {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Count Test").setMaster("local")
    val sc=new SparkContext(conf)

    countByKey2(sc)
  }


  def countByKey2(sc:SparkContext):Unit={
    val rdd=sc.parallelize(Array(("A","1"),("B",2),("C",3),("A",5)))
    val countByKeyRdd=rdd.countByKey
    countByKeyRdd.foreach(println)
  }
}
