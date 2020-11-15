package org.bing.spark.rdd.standard.transformation

import org.apache.spark.{SparkConf, SparkContext}

object reduceByKey {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("ReduceByKey Test").setMaster("local")
    val sc=new SparkContext(conf)

  }

  def reduceByKey1(sc:SparkContext):Unit={
    val rdd=sc.makeRDD(Array(("A",0),("A",2),("B",3),("B",4),("C",6)))
    val reduceByKeyRdd=rdd.reduceByKey((x,y)=>x+y)
    reduceByKeyRdd.foreach(println)
  }

//  def reduceByKey2(sc:SparkContext):Unit={
//    val rdd=sc.textFile(Constant.LoCAL_FILE_PREX+"/data/rdd/wordData.log")
//    println(Constant.LoCAL_FILE_PREX)
//    val reduceByKeyRdd=rdd.flatMap(line=>line.split("\\s+")).map(x=>(x,1)).reduceByKey((x,y)=>x+y)
//    reduceByKeyRdd.foreach(println)
//  }
}
