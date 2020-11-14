package org.bing.rdd.apply

import org.apache.spark.{SparkConf, SparkContext}

object 每行最多单词数 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("MaxLine Word").setMaster("local")
    val sc=new SparkContext(conf)

    val rdd=sc.textFile("E:\\data\\spark\\rdd\\test\\read\\word.log")
    val maxLineWordNum=rdd.map(line=>line.split(" ").length).reduce((a,b)=> if (a>b) a else b)
    println(maxLineWordNum)
  }

}
