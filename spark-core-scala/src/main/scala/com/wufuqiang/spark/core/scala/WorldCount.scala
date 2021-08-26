package com.wufuqiang.spark.core.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WorldCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wufuqiang").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val row: RDD[String]
//      = sc.textFile(args(0))
      = sc.textFile("/Users/user/myproject/spark-base/spark-core-test.data")

    val worldCount: RDD[(String, Int)] = row.flatMap(_.split(" "))
      .filter(!"".equals(_))
      .map((_, 1)).reduceByKey(_ + _)
      .sortBy(_._2,false)

//    worldCount.saveAsTextFile(args(1))
    val res: Array[(String, Int)] = worldCount.collect()
//    val strings: Array[String] = row.collect()

    for(s <- res){
      println(s)
    }

    sc.stop();
  }

}
