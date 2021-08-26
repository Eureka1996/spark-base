package com.wufuqiang.spark.core.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortByDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wufuqiang").setMaster("local[*]")
    conf.set("spark.files.overwrite","true")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(3, 6, 2, 7, 3, 1), 2)
    // 每个分区内都是排好序的
    val sortedRdd = rdd1.sortBy(item => item)

    val rdd3 = sc.makeRDD(List(("1",1),("11",2),("2",3)), 2)

    // 降序排序
    val rdd4 = rdd3.sortBy(item => item._1.toInt,false)

    sortedRdd.saveAsTextFile("output");

    rdd4.saveAsTextFile("output1")

    sc.stop();
  }

}
