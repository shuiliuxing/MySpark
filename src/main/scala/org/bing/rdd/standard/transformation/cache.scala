package org.bing.rdd.standard.transformation

import org.apache.spark.{SparkConf, SparkContext}

object cache {
  /*
  适用场景：
  （1）要求计算速度快
  （2）集群资源要足够大
  （3）cache的数据会多次出发action
  （4）先进行过滤，将过滤后的精准数据存放到内存中再进行操作
   */
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Cache Test").setMaster("local")
    val sc=new SparkContext(conf)
    cache1(sc)
  }

  def cache1(sc:SparkContext):Unit={
    val rdd=sc.textFile("E:\\data\\spark\\rdd\\test\\read\\ml-25m\\genome-scores.csv")
    rdd.cache()
    val count1=rdd.count
    println("count1："+count1) //耗时10575

    val count2=rdd.count
    println("count2："+count2) //耗时411

    val count3=rdd.count
    println("count3："+count3) //耗时273
  }
}
