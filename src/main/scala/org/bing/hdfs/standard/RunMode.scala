package org.bing.hdfs.standard

import org.apache.spark.{SparkConf, SparkContext}

object RunMode {
  def main(args:Array[String]):Unit={
    //winToWin();
    winToWinMultipleFile()
    //hdfsToLocal();
    //hdfsToHdfs();
  }

  //加载win，保存到win
  def winToWin():Unit={
    val conf=new SparkConf().setAppName("RunModeTest").setMaster("local")
    val sc=new SparkContext(conf)
    val fileRdd=sc.textFile("E:\\data\\spark\\rdd\\test\\read\\movies.csv")
    fileRdd.saveAsTextFile("E:\\data\\spark\\rdd\\test\\write\\movie")
  }

  def winToWinMultipleFile():Unit={
    val conf=new SparkConf().setAppName("RunModeTest").setMaster("local")
    val sc=new SparkContext(conf)
    val fileRdd=sc.textFile("E:\\data\\spark\\rdd\\test\\read\\movies.csv,E:\\data\\spark\\rdd\\test\\read\\movies1.csv")
    fileRdd.repartition(1).saveAsTextFile("E:\\data\\spark\\rdd\\test\\write\\movie")
  }

  //加载hdfs，保存到本地
  def hdfsToLocal():Unit={
    val conf=new SparkConf().setAppName("RunMode Test")
    val sc=new SparkContext(conf)
    val fileRdd=sc.textFile("hdfs://master:9000/hadoop/input/wordcount.txt")
    fileRdd.saveAsTextFile("file:///root/test/rdd/write/wordcount")
  }

  //加载hdfs，保存到hdfs
  def hdfsToHdfs():Unit={
    val conf=new SparkConf().setAppName("RunMode Test")
    val sc=new SparkContext(conf)
    val fileRdd=sc.textFile("hdfs://master:9000/hadoop/input/wordcount.txt")
    fileRdd.repartition(1).saveAsTextFile("hdfs://master:9000/hadoop/output/wordcount")
  }
}
