package org.bing.spark.rdd.standard.transformation

import org.apache.spark.{SparkConf, SparkContext}

object takeSample {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("TakeSample Test").setMaster("local")
    val sc=new SparkContext(conf)

    takeSample1(sc)
  }

  def takeSample1(sc:SparkContext):Unit={
    val rdd=sc.makeRDD(1 to 10)
    val sampleRdd=rdd.takeSample(true,4) //有放回抽样，采取数量4
    sampleRdd.foreach(println)
  }
}
