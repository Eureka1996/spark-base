package com.wufuqiang.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author: Wu Fuqiang
 * @create: 2021-09-02 23:18
 *
 */
object FirstDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLDemo")

    val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val df:DataFrame = sparkSession.read.json("/Users/user/myproject/spark-base/itemcf_test_data_1.json")

    df.printSchema()
    df.show()

    df.createOrReplaceTempView("item")

    sparkSession.sql("select * from item limit 2").show()

    val df3 = sparkSession.sql("select avg(rating),sum(rating) from item")
    df3.printSchema()
    df3.show()

    // 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
    import sparkSession.implicits._
    df.select("rating","userId").show()
    df.select($"rating"+1).show()
    df.select('rating+1).show()


    //DataSet
    val seq = Seq(1, 2, 3, 4, 5)
    val ds:Dataset[Int] = seq.toDS()
    ds.show()

    // RDD <=> DataFrame
    val rdd = sc.makeRDD(List((1, "wufuqiang", 12), (1, "wufuqiang", 12),
      (1, "wufuqiang", 12), (1, "wufuqiang", 12)))

    val df4:DataFrame = rdd.toDF("id", "name", "age")
    df4.show()

    val rdd1:RDD[Row] = df4.rdd

    // DataFrame <=> Dataset

    val ds2:Dataset[User] = df4.as[User]
    val df5 = ds2.toDF()

    // RDD <=> Dataset
    val ds3:Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()

    val rdd2 = ds3.rdd

    sparkSession.close()

  }

  case class User(id:Int,name:String,age:Int)

}

