package org.bing.rdd.standard.transformation

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("MapPartitionsWithIndex Test").setMaster("local")
    val sc=new SparkContext(conf)
    mapPartitionsWithIndex2(sc)
  }

  def func(index:Int, it:Iterator[Int]):Iterator[String]={
    it.map(x=>s"part:$index, ele:$x")
  }
  def mapPartitionsWithIndex1(sc:SparkContext):Unit={
    val rdd=sc.parallelize(List(1,2,3,4,5,6),2)
    val mapPartitionsWithIndexRdd=rdd.mapPartitionsWithIndex(func)
    mapPartitionsWithIndexRdd.foreach(println)
  }

  def mapPartitionsWithIndex2(sc:SparkContext):Unit={
    val rdd=sc.parallelize(Array("tom1","tom2","tom3","tom4","tom5","tom6"),3)
    val mapPartitionsWithIndexRdd=rdd.mapPartitionsWithIndex((index,x)=>{
      val list=ListBuffer[String]()
      while(x.hasNext){
        list+="part:"+index+", ele:"+x.next
      }
      list.iterator
    })
    mapPartitionsWithIndexRdd.foreach(println)
  }
}
