package org.bing.spark.sparkmlib.basix

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Vector {
  def main(args: Array[String]): Unit = {
    //密集向量
    val vd:Vector=Vectors.dense(2,0, 6);
    println(vd(2))

    //稀疏向量
    val vs:Vector=Vectors.sparse(4,Array(0,1,2,3),Array(9,5,2,7))
    println(vs(2))

    //向量标签（密集）
    val lpVd:LabeledPoint=LabeledPoint(3,vd)
    println(lpVd.features)
    println(lpVd.label)
    //向量标签（稀疏）
    val lpVs:LabeledPoint=LabeledPoint(3,vs)
    println(lpVs.features)
    println(lpVs.label)

    //loadLibSVMFile
    val conf:SparkConf=new SparkConf().setAppName("RunMode").setMaster("local")
    val sc:SparkContext=new SparkContext(conf)
    val rddLP:RDD[LabeledPoint]=MLUtils.loadLibSVMFile(sc,"E://data//spark//sparkmlib//loadLibSVMFile.txt")
    rddLP.foreach(println)
  }
}
