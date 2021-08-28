package com.wufuqiang.spark.core.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AverageByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wufuqiang").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val raw1 = sc.makeRDD(List(("1",1),("11",2),("2",3),("1",4),("11",5),("2",6)), 2)

    val rdd2:RDD[(String,(Int,Int))] = raw1.aggregateByKey((0, 0))(
      (x, y) => (x._1 + y, x._2 + 1),
      (x, y) => (x._1 + y._1, x._2 + y._2)
    )

    rdd2.collect().foreach(println)

    val rdd3 = rdd2.map(item => (item._1, item._2._1 * 1.0 / item._2._2))

    rdd3.collect().foreach(println)

    /**
     * combineByKey：方法需要三个参数
     *  第一个参数表示：将相同key的第一个数据进行结构的转换
     */
    val rdd4:RDD[(String,(Int,Int))] = raw1.combineByKey(
      v => (v, 1),
      (i1: (Int, Int), i2) => (i1._1 + i2, i1._2 + 1),
      (i1: (Int, Int), i2: (Int, Int)) => (i1._1 + i2._1, i1._2 + i2._2)
    )

    rdd4.map(item => (item._1,item._2._1*1.0/item._2._2)).collect().foreach(println)


    sc.stop();
  }

}
