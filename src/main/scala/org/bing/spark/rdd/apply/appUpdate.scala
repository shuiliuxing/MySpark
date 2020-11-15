package org.bing.spark.rdd.apply

import org.apache.spark.{SparkConf, SparkContext}
//数据格式
//日期，姓名，app，下载渠道，地区，版本号
//2020-05-14,张三,王者荣耀,华为应用,北京,v1.0
//2020-05-14,李四,王者荣耀,应用宝,北京,v1.2
//2020-05-14,张三,王者荣耀,华为应用,天津,v1.2
//2020-05-14,张三,王者荣耀,小米应用,天津,v2.0
//2020-05-14,王五,阴阳师,app store,上海,v1.8
//2020-05-14,张三,王者荣耀,小米应用,天津,v2.0
//2020-05-14,王五,阴阳师,app store,上海,v1.9
//2020-05-15,王五,阴阳师,app store,上海,v2.0
//2020-05-15,王五,阴阳师,app store,上海,v2.3
//2020-05-15,张三,王者荣耀,华为应用,北京,v2.0
//2020-05-15,李四,王者荣耀,应用宝,北京,v1.2
//2020-05-15,李四,王者荣耀,应用宝,北京,v1.5
//2020-05-15,王五,阴阳师,app store,上海,v2.9

//需求：不考虑地区，列出版本升级情况
//结果格式：日期，姓名，app，下载渠道，升级前版本，升级后版本

//示例：
//2020-05-15,张三,王者荣耀,华为应用,北京,v1.0
//2020-05-15,张三,王者荣耀,华为应用,天津,v1.2
//2020-05-15,张三,王者荣耀,华为应用,天津,v2.0
//结果：
//2020-05-15,张三,王者荣耀,应用宝,v1.0,v1.2
//2020-05-15,张三,王者荣耀,应用宝,v1.2,v2.0


object appUpdate {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("App Update").setMaster("local")
    val sc=new SparkContext(conf)

    val rdd=sc.textFile("E:\\data\\spark\\rdd\\test\\read\\app.log")
    val mapRdd=rdd.filter(line=>line.contains("v")).map(line=>{
      val arr=line.split(",",-1)
        ((arr(0),arr(1),arr(2),arr(3)), arr(5))
    })
    val keyRdd=mapRdd.groupByKey()
    val filterRdd=keyRdd.mapValues(line=>line.toList.distinct).filter(line=>line._2.length>1)
    val zipRdd=filterRdd.mapValues(line=>line.zip(line.tail))
    val resultRdd=zipRdd.flatMap(line=>{
      line._2.map(x=>{
        (line._1._1, line._1._2, line._1._3, line._1._4, x._1, x._2)
      })
    })
    resultRdd.foreach(println)
  }
}
