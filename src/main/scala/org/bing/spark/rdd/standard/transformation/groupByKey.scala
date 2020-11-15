package org.bing.spark.rdd.standard.transformation

import org.apache.spark.{SparkConf, SparkContext}

object groupByKey {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("GroupByKey Test").setMaster("local")
    val sc=new SparkContext(conf)

    groupByKey2(sc)
  }

  //简单1
  def groupByKey1(sc:SparkContext):Unit={
    val rdd=sc.parallelize(Array("one","two","three","three","three","one"))
    val mapRdd=rdd.map(line=>(line,1))
    val groupByKeyRdd=mapRdd.groupByKey()
    groupByKeyRdd.foreach(println)
  }

  //简单2
  def groupByKey2(sc:SparkContext):Unit={
    val rdd=sc.parallelize(Array(("A",0),("A",2),("B",1),("B",8),("C",3)))
    val groupByKeyRdd=rdd.groupByKey()
    groupByKeyRdd.foreach(println)
  }



}
