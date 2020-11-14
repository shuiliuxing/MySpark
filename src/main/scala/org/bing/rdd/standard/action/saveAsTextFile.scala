package org.bing.rdd.standard.action

import org.apache.spark.{SparkConf, SparkContext}

object saveAsTextFile {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Count Test").setMaster("local")
    val sc=new SparkContext(conf)
    saveAsTextFile1(sc)
  }

  def saveAsTextFile1(sc:SparkContext):Unit={
    val rdd=sc.makeRDD(Seq(("A",1),("B",2),("C",3),("D",4),("E",5),("F",6)),4)
    rdd.saveAsTextFile("E:\\data\\spark\\rdd\\test\\write\\saveAsTextFile")
  }
}
