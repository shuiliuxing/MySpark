package org.bing.rdd.standard.action

import org.apache.spark.{SparkConf, SparkContext}
/*
  升序
 */
object takeOrdered {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("TakeOrdered Test").setMaster("local")
    val sc=new SparkContext(conf)
    takeOrderedTest(sc)
  }

  def takeOrderedTest(sc:SparkContext):Unit={
    val rdd=sc.makeRDD(Seq(10,4,2,12,3,1))
    val takeOrderedRdd=rdd.takeOrdered(2)
    takeOrderedRdd.foreach(println)
  }
}
