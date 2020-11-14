package org.bing.rdd.standard.transformation

import org.apache.spark.{SparkConf, SparkContext}

object mapPartitions {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("MapPartitions Test").setMaster("local")
    val sc=new SparkContext(conf)
    mapPartitions3(sc)
  }

  //简单1
  def mapPartitions1(sc:SparkContext):Unit={
    val rdd=sc.parallelize(Array(1,2,3,4,5,6), 2)
    val mapPartitionsRdd=rdd.mapPartitions(_.map(_+1))
    mapPartitionsRdd.foreach(println)
  }

  //简单2
  def doubleFunc(iter:Iterator[Int]): Iterator[(Int,Int)]={
    var res=List[(Int,Int)]()
    while(iter.hasNext){
      val cur=iter.next
      res.::=(cur, cur*2)
    }
    res.iterator
  }
  def mapPartitions2(sc:SparkContext):Unit={
    val rdd=sc.parallelize(1 to 9, 3)
    val mapPartitionsRdd=rdd.mapPartitions(doubleFunc)
    println(mapPartitionsRdd.collect().mkString)
  }

  //简单3
  def menaFunc(iter:Iterator[Int]):Iterator[(Int,Int)]={
    var x,y=0
    var res=List[(Int,Int)]()
    while(iter.hasNext){
      val cur=iter.next
      x+=cur
      y+=1
    }
    res.::=(x,y)
    res.iterator
  }
  def reduceFunc(x:(Int,Int),y:(Int,Int)):(Int,Int)={
    (x._1+y._1, x._2+y._2)
  }
  def mapPartitions3(sc:SparkContext):Unit={
    val rdd=sc.parallelize(List(1,2,3,4),3)
    val sum=rdd.mapPartitions(menaFunc).reduce(reduceFunc)
    println(sum._1/sum._2.asInstanceOf[Float])
  }
}
