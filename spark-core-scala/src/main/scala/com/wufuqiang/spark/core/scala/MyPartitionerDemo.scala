package com.wufuqiang.spark.core.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object MyPartitionerDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wufuqiang").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataRaw1:RDD[(String,Int)] = sc.makeRDD(List(
      ("nba", 1),
      ("cba", 2),
      ("wnba", 3),
      ("wcba", 4)
    ),3)

    val partitionedRdd = dataRaw1.partitionBy(new MyPartitioner)

    partitionedRdd.saveAsTextFile("output")


    sc.stop();
  }

  class MyPartitioner extends Partitioner{

    // 分区数量
    override def numPartitions: Int = 3

    // 返回数据的分区索引，从0开始
    override def getPartition(key: Any): Int = {
      key match{
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }

    }
  }

}
