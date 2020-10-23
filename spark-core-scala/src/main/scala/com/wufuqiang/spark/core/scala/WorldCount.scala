package com.wufuqiang.spark.core.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WorldCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("local")
    val sc = new SparkContext(conf)

    val row: RDD[String] = sc.textFile("/Users/user/myproject/spark-base/spark-core-test.data")

    val worldCount: RDD[(String, Int)] = row.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    val res: Array[(String, Int)] = worldCount.collect()
//    val strings: Array[String] = row.collect()

    for(s <- res){
      println(s)
    }

    sc.stop();
  }

}
