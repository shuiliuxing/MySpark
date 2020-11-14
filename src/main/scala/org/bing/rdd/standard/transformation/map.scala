package org.bing.rdd.standard.transformation

import org.apache.spark.{SparkConf, SparkContext}

object map {
  def main(args:Array[String]): Unit ={
    val conf=new SparkConf().setAppName("Map Test").setMaster("local")
    val sc=new SparkContext(conf)
    map3(sc)

  }

  //简单1
  def map1(sc:SparkContext):Unit={
    val rdd=sc.parallelize(List(1,2,3,4))
    val rdd1=rdd.map(line=>line+1)
    rdd1.foreach(println)
  }

  //简单2
  def map2(sc:SparkContext):Unit={
    val rdd=sc.parallelize(List(("spark",0),("hadoop",10),("hadoop",4),("spark",4)))
    val mapRdd=rdd.map(line=>(line,1))
    mapRdd.foreach(println)
  }

  //简单3
  def map3(sc:SparkContext):Unit={
    val rdd=sc.textFile("E:\\data\\spark\\rdd\\test\\read\\movies.csv")
    val arr=rdd.map(line=>line.split(",")).collect()
    for(i <- 0 to 2){
      for(j <- 0 to 2){
        print(arr(i)(j)+"\t")
      }
      println()
    }
  }
}
