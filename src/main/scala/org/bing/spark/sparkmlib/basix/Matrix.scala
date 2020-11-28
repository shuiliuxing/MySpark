package org.bing.spark.sparkmlib.basix

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRow, IndexedRowMatrix, MatrixEntry, RowMatrix}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.rdd.RDD

object Matrix {
  def main(args: Array[String]): Unit = {
    //本地矩阵（2行3列）
    val mx:Matrix=Matrices.dense(2,3,Array(1,2,3,4,5,6))
    println(mx)

    //行矩阵
    val conf:SparkConf=new SparkConf().setAppName("Matrix Test").setMaster("local")
    val sc:SparkContext=new SparkContext(conf)
    val rdd:RDD[String]=sc.textFile("E://data//spark//sparkmlib//RowMatrix.txt")
    val rddVec:RDD[Vector]=rdd.map(_.split(" ").map(_.toDouble))
                               .map(arr => Vectors.dense(arr))
    val rm:RowMatrix=new RowMatrix(rddVec)
    println(rm.numRows())   //行数
    println(rm.numCols())   //列数
    //带索引行矩阵
    val rddIR:RDD[IndexedRow]=rdd.map(_.split(" ").map(_.toDouble))
                                  .map(arr=>Vectors.dense(arr))
                                  .map(vd=>new IndexedRow(vd.size,vd))
    val irMx:IndexedRowMatrix=new IndexedRowMatrix(rddIR)
    println(irMx.getClass)
    irMx.rows.foreach(println)

    //坐标矩阵
    val mxEntryRdd:RDD[MatrixEntry]=rdd.map(_.split(" ").map(_.toDouble))
                   .map(arr=>(arr(0).toLong, arr(1).toLong, arr(2).toLong))
                   .map(x=>new MatrixEntry(x._1, x._2, x._3))
    val cdm=new CoordinateMatrix(mxEntryRdd)
    cdm.entries.foreach(println)
  }
}
