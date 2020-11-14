package org.bing.rdd.standard.transformation

import org.apache.spark.{SparkConf, SparkContext}

object sortByKey {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("SortByKey Test").setMaster("local")
    val sc=new SparkContext(conf)

    sortByKey2(sc)
  }

  def sortByKey1(sc:SparkContext):Unit={
    val rdd=sc.makeRDD(List((60,"张三"), (70,"李四"), (80,"王五"), (55,"赵六"), (45,"郭七"), (75,"林八")))
    val sortByKeyRdd=rdd.sortByKey()
    sortByKeyRdd.foreach(println)
  }

  def sortByKey2(sc:SparkContext):Unit={
    val rdd=sc.makeRDD(List((60,"张三"), (70,"李四"), (80,"王五"), (55,"赵六"), (45,"郭七"), (75,"林八")))
    val sortByKeyRdd=rdd.sortByKey(false) //降序
    sortByKeyRdd.foreach(println)
  }
}
