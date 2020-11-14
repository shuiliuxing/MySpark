package org.bing.rdd.standard.transformation

import org.apache.spark.{SparkConf, SparkContext}

object coalesce {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Coalesce Test").setMaster("local")
    val sc=new SparkContext(conf)
    coalesce2(sc)
  }

  /*
    def coalesce(numPartitions: Int, shuffle: Boolean = false)(implicit ord: Ordering[T] = null): RDD[T]
    参数1：重分区数目
    参数2: 是否进行shuffle，默认false
    功能：将RDD进行重分区，使用HashPartitioner
   */
  def coalesce1(sc:SparkContext):Unit={
    val rdd=sc.textFile("E:\\data\\spark\\rdd\\test\\read\\ml-25m\\genome-scores.csv")
    println("分区数："+rdd.partitions.size) //13
    val coalesceRdd=rdd.coalesce(2)//减少分区数
    println("新分区数："+coalesceRdd.partitions.size) //2
    coalesceRdd.saveAsTextFile("E:\\data\\spark\\rdd\\test\\write\\ml-25m")//2
  }

  def coalesce2(sc:SparkContext):Unit={
    val rdd=sc.textFile("E:\\data\\spark\\rdd\\test\\read\\ml-25m\\genome-scores.csv")
    println("分区数："+rdd.partitions.size) //13
    val coalesceRdd=rdd.coalesce(20,true)//将shuffle设为true，增加分区数到200
    println("新分区数："+coalesceRdd.partitions.size) //20
    coalesceRdd.saveAsTextFile("E:\\data\\spark\\rdd\\test\\write\\ml-25m")//20
  }
}
