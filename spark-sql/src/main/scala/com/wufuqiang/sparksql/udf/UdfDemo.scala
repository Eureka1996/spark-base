package com.wufuqiang.sparksql.udf

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author: Wu Fuqiang
 * @create: 2021-09-02 23:18
 *
 */
object UdfDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLDemo")

    val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    import sparkSession.implicits._

    val sc = sparkSession.sparkContext


    val rdd = sc.makeRDD(List((1, "wufuqiang", 12), (1, "wufuqiang", 12),
      (1, "wufuqiang", 12), (1, "wufuqiang", 12)))

    val df:DataFrame = rdd.toDF("id", "name", "age")
    df.show()

    df.createOrReplaceTempView("user")

    val df2 = sparkSession.sql("select id,name,age from user")
    df2.show()

    sparkSession.udf.register("prefixName",(name:String) => "userName:"+name)
    val df3 = sparkSession.sql("select id,prefixName(name) as name,age from user")

    df3.show()

    sparkSession.close()

  }

  case class User(id:Int,name:String,age:Int)

}

