package com.wufuqiang.spark.core.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ForeachDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wufuqiang").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val raw1 = sc.makeRDD(List[Int]())

    val user = new User()

    // rdd算子中传递的函数是包含闭包操作的，那么就会进行检测功能，即闭包检测
    raw1.foreach(num =>{
      println(s"age = ${user.age+num}")
    })

    sc.stop();
  }

}

class User{
  var age:Int = 30
}
