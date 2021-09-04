package com.wufuqiang.sparksql.udaf

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession, TypedColumn, functions}
import org.apache.spark.sql.expressions.Aggregator

/**
 * @author: Wu Fuqiang
 * @create: 2021-09-02 23:18
 *
 */
object UdafDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLDemo")

    val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    import sparkSession.implicits._

    val sc = sparkSession.sparkContext


    val rdd = sc.makeRDD(List((1, "wufuqiang", 11), (1, "wufuqiang", 12),
      (1, "wufuqiang", 13), (1, "wufuqiang", 14)))

    val df:DataFrame = rdd.toDF("id", "name", "age")
    df.show()

    df.createOrReplaceTempView("user")

    val df2 = sparkSession.sql("select id,name,age from user")
    df2.show()

    sparkSession.udf.register("ageAvg",new MyAvgUdaf)
    // sparkSession.udf.register("ageAvg2",functions.udaf(new MyAvgUdaf2())) // spark v3.0.0
    val df3 = sparkSession.sql("select ageAvg(age) from user")

    df3.show()

    sparkSession.sql("select id,ageAvg(age) from user group by id").show()

    println("#"*50)
    val ds = df.as[User]
    val udafCol:TypedColumn[User,Double] = new MyAvgUdaf3().toColumn
    ds.select(udafCol).show()

    sparkSession.close()

  }

  /**
   * 自定义聚合函数类：计算年龄的平均值
   * 1、继承UserDefinedAggregateFunction
   * 2、重写方法
   */
  class MyAvgUdaf extends UserDefinedAggregateFunction{
    // 输入的数据结构结构
    override def inputSchema: StructType = {
      StructType(
        Array(
          StructField("age",LongType)
        )
      )
    }

    // 缓冲区的数据结构
    override def bufferSchema: StructType = {
      StructType(
        Array(
          StructField("total",LongType),
          StructField("count",LongType)
        )
      )
    }
    // 函数计算结果的数据类型
    override def dataType: DataType = DoubleType

    //函数的稳定性
    override def deterministic: Boolean = true

    // 缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
//      buffer(0) = 0L;
//      buffer(1) = 0L;
      buffer.update(0,0L)
      buffer.update(1,0L)
    }

    // 根据输入的值更新缓冲区数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0,buffer.getLong(0)+input.getLong(0))
      buffer.update(1,buffer.getLong(1)+1)
    }

    // 缓冲区数据合并，并行计算，有多个缓冲区
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0,buffer1.getLong(0) + buffer2.getLong(0))
      buffer1.update(1,buffer1.getLong(1) + buffer2.getLong(1))
    }

    // 计算，此例中为计算平均值
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0)*1.0/buffer.getLong(1)
    }
  }

  /**
   * 自定义聚合函数类：计算年龄的平均值
   * 1、继承org.apache.spark.sql.expressions.Aggregator，定义泛型：IN,BUF,OUT
   * 2、重写方法
   */
  case class AggBuff(var total:Long,var count:Long)
  class MyAvgUdaf2 extends Aggregator[Long,AggBuff,Double]{
    // 初始值，或称零值
    // 缓冲区的初始化
    override def zero: AggBuff = AggBuff(0L,0L)

    //根据输入的数据来更新缓冲区
    override def reduce(buff: AggBuff, in: Long): AggBuff = {
      buff.total = buff.total + in
      buff.count = buff.count + 1
      buff
    }

    // 合并缓冲区
    override def merge(buff1: AggBuff, buff2: AggBuff): AggBuff = {
      buff1.total = buff1.total + buff2.total
      buff1.count = buff1.count + buff2.count
      buff1
    }

    //计算结果
    override def finish(reduction: AggBuff): Double = {
      reduction.total*1.0/reduction.count
    }

    //缓冲区的编码操作
    override def bufferEncoder: Encoder[AggBuff] = Encoders.product

    //输出的编码操作
    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }
  case class User(id:Int,name:String,age:Int)

  class MyAvgUdaf3 extends Aggregator[User,AggBuff,Double]{
    // 初始值，或称零值
    // 缓冲区的初始化
    override def zero: AggBuff = AggBuff(0L,0L)

    //根据输入的数据来更新缓冲区
    override def reduce(buff: AggBuff, in: User): AggBuff = {
      buff.total = buff.total + in.age
      buff.count = buff.count + 1
      buff
    }

    // 合并缓冲区
    override def merge(buff1: AggBuff, buff2: AggBuff): AggBuff = {
      buff1.total = buff1.total + buff2.total
      buff1.count = buff1.count + buff2.count
      buff1
    }

    //计算结果
    override def finish(reduction: AggBuff): Double = {
      reduction.total*1.0/reduction.count
    }

    //缓冲区的编码操作
    override def bufferEncoder: Encoder[AggBuff] = Encoders.product

    //输出的编码操作
    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

}

