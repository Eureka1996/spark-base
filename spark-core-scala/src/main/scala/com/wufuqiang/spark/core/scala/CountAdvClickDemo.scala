package com.wufuqiang.spark.core.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CountAdvClickDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wufuqiang").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataRDD = sc.textFile("")

    val mapRDD = dataRDD.map(
      line => {
        val datas = line.split(",")
        ((datas(1), datas(4)), 1)
      }
    )

    val reduceRDD = mapRDD.reduceByKey(_ + _)

    val newMapRDD = reduceRDD.map {
      case ((prv, ad), sum) => {
        (prv, (ad, sum))
      }
    }


    val groupRDD:RDD[(String,Iterable[(String,Int)])] = newMapRDD.groupByKey()

    val resultRDD = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )


    resultRDD.collect().foreach(println)

    sc.stop();
  }

}
