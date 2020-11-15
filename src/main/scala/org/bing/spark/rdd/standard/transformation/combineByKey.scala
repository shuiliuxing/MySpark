package org.bing.spark.rdd.standard.transformation

import org.apache.spark.{SparkConf, SparkContext}

object combineByKey {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("CombineByKey Test").setMaster("local")
    val sc=new SparkContext(conf)

    combineByKey3(sc)
  }

  //简单1
  def combineByKey1(sc:SparkContext):Unit={
    val rdd=sc.makeRDD(Array(("A",1),("A",2),("B",3),("B",4),("C",6)))
    val combineByKeyRdd=rdd.combineByKey((v:Int)=>v+"-",(c:String,v:Int)=>c+"@"+v,(c1:String,c2:String)=>c1+"$"+c2)
    combineByKeyRdd.foreach(println)
  }

  //简单2
  def combineByKey2(sc:SparkContext):Unit={
    val rdd=sc.parallelize(Array((1, 1.0), (1, 2.0), (1, 3.0), (2, 4.0), (2, 5.0), (2, 6.0)))
    val combineByKeyRdd=rdd.combineByKey((v:Double)=>(v,1), (c:(Double,Int),v:Double)=>(c._1+v,c._2+1),(c1:(Double,Int),c2:(Double,Int))=>(c1._1+c2._1, c1._2+c2._2))
    combineByKeyRdd.foreach(println)
  }

  //简单3
  def combineByKey3(sc:SparkContext):Unit={
    val rdd=sc.parallelize(List(("A", 3), ("A", 9), ("A", 12),("B", 4), ("B", 10), ("B", 11)), 2)
    val combineByKeyRdd=rdd.combineByKey(
      (x:Int)=>(x,1),
      (acc:(Int,Int), x)=>(acc._1+x, acc._2+1),
      (p1:(Int,Int), p2:(Int,Int))=> (p1._1+p2._1, p1._2+p2._2)
    )
    combineByKeyRdd.foreach(println)
  }

  //简单4
  def combineByKey4(sc:SparkContext):Unit={
    val rdd=sc.parallelize(Array(("coffee",1), ("coffee",2), ("panda",3), ("coffee",9)))
    val combineByKeyRdd=rdd.combineByKey(score=>(1,score), (c:(Int,Int),newScore:Int)=>(c._1+1,c._2+newScore), (c1:(Int,Int),c2:(Int,Int))=>(c1._1+c2._1,c1._2+c2._2))
    val averageRdd=combineByKeyRdd.map{
      //case(key,value)=>(key,value._2/value._1)
      case(name,(num,score))=>(name,score/num)
    }
    averageRdd.foreach(println)
  }

  //简单5
  def combineByKey5(sc:SparkContext):Unit={
    val rdd=sc.parallelize(Array((("1","011"),1), (("1","012"),1), (("2","011"),1), (("2","013"),1), (("2","014"),1)))
    val combineByKeyRdd=rdd.map(x=>(x._1._1, (x._1._2,1))).combineByKey(
      (v:(String,Int))=> (v:(String,Int)),
      (acc:(String,Int), v:(String,Int))=> (acc._1+":"+v._1, acc._2+v._2),
      (p1:(String,Int), p2:(String,Int))=> (p1._1+":"+p2._1, p1._2+p2._2)
    )
    combineByKeyRdd.foreach(println)
  }

}
