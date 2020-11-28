package org.bing.spark.rdd.standard.transformation

import org.apache.spark.{SparkConf, SparkContext}

object sample {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Sample Test").setMaster("local")
    val sc=new SparkContext(conf)
    sample1(sc)
  }

  //简单1--（有/无放回抽样，抽样比例--注意不一定精确，随机数种子）
  def sample1(sc:SparkContext):Unit={
    val rdd=sc.parallelize(List(2,3,7,4,8,9))
    val sampleRdd=rdd.sample(false,0.5)
    sampleRdd.foreach(println)
  }

  //简单2
  def sample2(sc:SparkContext):Unit={
    val rdd=sc.parallelize(List(2,3,7,4,8))
    val sampleRdd=rdd.sample(true,2)
    sampleRdd.foreach(println)
  }

  //简单3
  def sample3(sc:SparkContext):Unit={
    val rdd=sc.makeRDD(List(1,2,3,7,4,5,8))
    val sampleRdd=rdd.sample(false,0.25, System.currentTimeMillis())
    sampleRdd.foreach(println)
  }
}
