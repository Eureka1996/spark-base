package com.wufuqiang.sparksql.jdbc

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

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

    val df = sparkSession.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark_sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123")
      .option("dbtable", "user")
      .load()

    df.write
      .format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/spark_sql")
      .option("driver","com.mysql.jdbc.Driver")
      .option("user","root")
      .option("password","123")
      .option("dbtable","user")
      .mode(SaveMode.Append)
      .save()

    sparkSession.close()

  }

  case class User(id:Int,name:String,age:Int)

}

