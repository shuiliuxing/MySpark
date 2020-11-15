package org.bing.spark.dataframe.basicfunc

import org.apache.spark.sql.SparkSession

object toDF {
  def main(args: Array[String]): Unit = {
    val ss=SparkSession.builder().appName("Select Test").master("local").getOrCreate()
    toDF2(ss)
  }

  //RDD转DataFrame
  def toDF1(ss:SparkSession):Unit={
    val rdd=ss.sparkContext.textFile("E:\\data\\spark\\rdd\\test\\read\\app1.log")
    val mapRdd=rdd.map(line=>line.split(",")).map{arr=>(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5))}
    import ss.implicits._
    val df=mapRdd.toDF("Date","Name","APP","DownLoad","Area","Version")
    df.show()
  }

  //DataSet转DataFrame
  def toDF2(ss:SparkSession):Unit={
    import ss.implicits._
    val ds=ss.createDataset(Seq(
      ("张三",21,11111.11),
      ("李四",22,22222.22),
      ("王五",23,33333.33),
      ("赵六",24,44444.44)
    ))
    val df=ds.toDF("Name","Age","Salary")
    df.show()
  }


}
