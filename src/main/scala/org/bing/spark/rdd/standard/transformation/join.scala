package org.bing.spark.rdd.standard.transformation

import org.apache.spark.{SparkConf, SparkContext}

object join {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Join Test").setMaster("local")
    val sc=new SparkContext(conf)

    //join1(sc)
    //leftOuterJoin1(sc)
    rightOuterJoin1(sc)
  }

  //简单1
  def join1(sc:SparkContext):Unit={
    val productRdd=sc.parallelize(List((1,"苹果"),(2,"梨"),(3,"香蕉"),(4,"石榴")))
    val countRdd=sc.parallelize(List((1,7),(2,3),(3,8),(4,3),(5,9)))
    val joinRdd=productRdd.join(countRdd)
    joinRdd.foreach(println)
  }

  def leftOuterJoin1(sc:SparkContext):Unit={
    val productRdd=sc.parallelize(List((1,"苹果"),(2,"梨"),(3,"香蕉"),(4,"石榴")))
    val countRdd=sc.parallelize(List((1,7),(2,3),(3,8),(4,3),(5,9)))
    val joinRdd=productRdd.leftOuterJoin(countRdd)
    joinRdd.foreach(println)
  }

  def rightOuterJoin1(sc:SparkContext):Unit={
    val productRdd=sc.parallelize(List((1,"苹果"),(2,"梨"),(3,"香蕉"),(4,"石榴")))
    val countRdd=sc.parallelize(List((1,7),(2,3),(3,8),(4,3),(5,9)))
    val joinRdd=productRdd.rightOuterJoin(countRdd)
    joinRdd.foreach(println)
  }
}
