package org.bing.rdd.standard.transformation

import org.apache.spark.{SparkConf, SparkContext}

object aggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("AggregateByKey Test").setMaster("local")
    val sc=new SparkContext(conf)

    aggregateByKey2(sc)
  }

  def aggregateByKey1(sc:SparkContext):Unit={
    val rdd=sc.parallelize(List(("cat",3), ("cat", 1), ("mouse", 4), ("dog", 9), ("mouse", 2)),2)
    val aggregateByKeyRdd=rdd.aggregateByKey(100)(_+_, _+_)
    aggregateByKeyRdd.foreach(println)
  }

  def combOp(a:List[Int], b:List[Int]):List[Int]={
    a.:::(b)
  }
  def seqOp(a:List[Int], b:Int):List[Int]={
    a.::(b)
  }
  def aggregateByKey2(sc:SparkContext):Unit={
    val rdd=sc.parallelize(List((1,3),(1,2),(1,4),(2,3)))
    val aggregateByKeyRdd=rdd.aggregateByKey(List[Int]())(seqOp,combOp)
    aggregateByKeyRdd.foreach(println)
  }
}
