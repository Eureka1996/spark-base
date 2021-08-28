package com.wufuqiang.spark.core.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object JoinDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wufuqiang").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val raw1 = sc.makeRDD(List(("1",1),("11",2),("2",3),("1",4),("11",5),("2",6),("3",6)), 2)
    val raw2 = sc.makeRDD(List(("1",11),("11",22),("2",33),("1",44),("11",55),("2",66)), 2)

    val rdd1:RDD[(String,(Int,Int))] = raw1.join(raw2)

    rdd1.collect().foreach(println)

    sc.stop();
  }

}
