package org.bing.rdd.standard.action

import org.apache.spark.{SparkConf, SparkContext}

object first {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Count Test").setMaster("local")
    val sc=new SparkContext(conf)
    
    first2(sc)
  }

  def first1(sc:SparkContext):Unit={
    val rdd=sc.parallelize(Array(5,4,8,3,0))
    println(rdd.first())
  }

  def first2(sc:SparkContext):Unit={
    val rdd=sc.parallelize(Array("a","ab","abc","abcd"))
    val firstMap=rdd.map(line=>(line,line.length)).first
    println(firstMap)
  }

}
