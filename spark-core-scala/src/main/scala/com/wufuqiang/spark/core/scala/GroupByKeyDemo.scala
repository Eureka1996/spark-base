package com.wufuqiang.spark.core.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wufuqiang").setMaster("local[*]")
    conf.set("spark.files.overwrite","true")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(("1",1),("11",2),("2",3),("1",4),("11",5),("2",6)), 2)

    /**
     * groupByKey:将数据源中的数据，相同key的数据分在一个组中，形成一个对偶元组
     *            元组中的第一个元素就是key
     *            元组中的第二个元素就是相同key的value的集合
     */
    val rdd2:RDD[(String,Iterable[Int])] = rdd1.groupByKey()

    rdd2.collect().foreach(println)

    /**
     * groupBy:需要指定分组的key
     */
    val rdd3:RDD[(String,Iterable[(String,Int)])] = rdd1.groupBy(_._1)

    rdd3.collect().foreach(println)

    sc.stop();
  }

}
