package com.wufuqiang.spark.core.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WorldCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wufuqiang").setMaster("local[*]")
    val sc = new SparkContext(conf)
    wordcount1(sc)
    wordcount2(sc)
    wordcount3(sc)
    wordcount6(sc)
    wordcount7(sc)

    sc.stop();
  }

  def wordcount1(sc:SparkContext):Unit ={
    val data = sc.makeRDD(List("Hello Scala", "Hello world"))
    val words = data.flatMap(_.split(" "))
    val group:RDD[(String,Iterable[String])] = words.groupBy(word => word)
    val worldCount:RDD[(String,Int)] = group.mapValues(item => item.size)
    worldCount.collect().foreach(println)
  }

  def wordcount2(sc:SparkContext):Unit ={
    val data = sc.makeRDD(List("Hello Scala", "Hello world"))
    val words = data.flatMap(_.split(" ")).map((_,2))
    val group:RDD[(String,Iterable[Int])] = words.groupByKey()
    val worldCount:RDD[(String,Int)] = group.mapValues(item => item.sum)
    worldCount.collect().foreach(println)
  }

  def wordcount3(sc:SparkContext):Unit ={
    val data = sc.makeRDD(List("Hello Scala", "Hello world"))
    val words = data.flatMap(_.split(" ")).map((_,1))
    val worldCount:RDD[(String,Int)] = words.reduceByKey(_+_)
    worldCount.collect().foreach(println)
  }

  def wordcount4(sc:SparkContext):Unit ={
    val data = sc.makeRDD(List("Hello Scala", "Hello world"))
    val words = data.flatMap(_.split(" ")).map((_,1))
    val worldCount:RDD[(String,Int)] = words.aggregateByKey(0)(_+_,_+_)
    worldCount.collect().foreach(println)
  }

  def wordcount5(sc:SparkContext):Unit ={
    val data = sc.makeRDD(List("Hello Scala", "Hello world"))
    val words = data.flatMap(_.split(" ")).map((_,1))
    val worldCount:RDD[(String,Int)] = words.foldByKey(0)(_+_)
    worldCount.collect().foreach(println)
  }
  def wordcount6(sc:SparkContext):Unit ={
    val data = sc.makeRDD(List("Hello Scala", "Hello world"))
    val words = data.flatMap(_.split(" ")).map((_,1))
    val worldCount:RDD[(String,Int)] = words.combineByKey(item=>item,
      (x:Int,y) => x+y,
      (x:Int,y:Int) => x+y
    )
    worldCount.collect().foreach(println)
  }

  def wordcount7(sc:SparkContext):Unit ={
    val data = sc.makeRDD(List("Hello Scala", "Hello world"))
    val words = data.flatMap(_.split(" ")).map((_,1))
    val wordCount = words.countByKey()
    wordCount.foreach(println)
  }

  def wordcount8(sc:SparkContext):Unit ={
    val data = sc.makeRDD(List("Hello Scala", "Hello world"))
    val words = data.flatMap(_.split(" "))
    val wordCount = words.countByValue()
    wordCount.foreach(println)
  }
}
