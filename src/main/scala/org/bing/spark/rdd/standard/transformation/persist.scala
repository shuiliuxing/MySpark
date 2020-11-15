package org.bing.spark.rdd.standard.transformation

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object persist {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Persist Test").setMaster("local")
    val sc=new SparkContext(conf)
    persist1(sc)
  }

  def persist1(sc:SparkContext):Unit={
    val rdd=sc.textFile("E:\\data\\spark\\rdd\\test\\read\\ml-25m\\genome-scores.csv")
    rdd.persist(StorageLevel.MEMORY_AND_DISK)
    val start=System.currentTimeMillis()
    val count1=rdd.count
    System.out.println("count1："+count1)
    val end=System.currentTimeMillis()
    System.out.println("costtime："+(end-start)+"\n")

    val start2=System.currentTimeMillis()
    val count2=rdd.count
    System.out.println("count2："+count2)
    val end2=System.currentTimeMillis()
    System.out.println("costtime："+(end2-start2))

    val start3=System.currentTimeMillis()
    val count3=rdd.count
    System.out.println("count3："+count3)
    val end3=System.currentTimeMillis()
    System.out.println("costtime："+(end3-start3))
  }
}
