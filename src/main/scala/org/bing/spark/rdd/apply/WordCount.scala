package org.bing.spark.rdd.apply

import org.apache.spark.{SparkConf, SparkContext}
/*
  需求：统计每个元素的个数
  文本：
    a , b , c , d

    b , b , f , e

    a , a , c , f
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("WordCount Test").setMaster("local")
    val sc=new SparkContext(conf)

    val rdd=sc.textFile("E:\\data\\spark\\rdd\\test\\read\\wordCount.txt")
      .filter(!_.isEmpty)
      .flatMap(_.split(","))
      .map(x=>(x.trim,1))
      .reduceByKey(_+_)
      .sortByKey()
    rdd.foreach(println)
  }
}
