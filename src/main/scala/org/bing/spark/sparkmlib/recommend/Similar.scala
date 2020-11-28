package org.bing.spark.sparkmlib.recommend

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map

object Similar {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Similar Test").setMaster("local")
    val sc=new SparkContext(conf)
    //获取数据源 (用户、电影、评分)
    val userDataMap=Map[String,Map[String,Int]]()    //使用1个source嵌套map作为姓名、电影名和分值的存储
    userDataMap.put("张三", Map("泰坦尼克号" -> 2, "星际穿越" -> 3, "咒怨" -> 1, "侏罗纪世界" -> 0, "战狼" -> 1))
    userDataMap.put("李四", Map("泰坦尼克号" -> 1, "星际穿越" -> 2, "咒怨" -> 2, "侏罗纪世界" -> 1, "战狼" -> 4))
    userDataMap.put("王五", Map("泰坦尼克号" -> 2, "星际穿越" -> 1, "咒怨" -> 0, "侏罗纪世界" -> 1, "战狼" -> 4))
    userDataMap.put("赵六", Map("泰坦尼克号" -> 3, "星际穿越" -> 2, "咒怨" -> 0, "侏罗纪世界" -> 5, "战狼" -> 3))
    userDataMap.put("刘七", Map("泰坦尼克号" -> 5, "星际穿越" -> 3, "咒怨" -> 1, "侏罗纪世界" -> 1, "战狼" -> 2))
    val userArr=sc.parallelize(Array("张三","李四","王五","赵六","刘七"))      //用户
    userArr.foreach(user=>{
      println("李四相对于 "+user+" 的相似度是："+getCosSimilar(userDataMap,"李四", user))
    })
//    println( getCosSimilar(userDataMap, "李四","李四") )
//    println( getCosSimilar(userDataMap, "李四","王五") )
  }

  //计算2个用户之间的余弦相似性
  def getCosSimilar(userDataMap:Map[String,Map[String,Int]], user1:String, user2:String):Double={
    val user1Vector=userDataMap.get(user1).get.values.toVector   //获得第1个用户的评分
    val user2Vector=userDataMap.get(user2).get.values.toVector   //获取第2个用户的评分
    val Exy=user1Vector.zip(user2Vector).map(x=>x._1 * x._2).reduce(_+_).toDouble
    val Ex2=math.sqrt(
      user1Vector.map(num=>{math.pow(num,2)}).reduce(_+_)
    )
    val Ey2=math.sqrt(
      user2Vector.map(num=>{math.pow(num,2)}).reduce(_+_)
    )
    Exy/(Ex2 * Ey2)
  }

}