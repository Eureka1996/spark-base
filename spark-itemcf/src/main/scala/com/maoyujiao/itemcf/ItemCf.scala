package com.maoyujiao.itemcf

import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object ItemCf {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("wufq")//.setMaster()

    val spark = SparkSession.builder()
      .config(sparkConf).getOrCreate()

    import spark.implicits._

    val schemas = Seq("userId","itemId","categoryId","behaviorType","timestamp")
    val userBebaviorDf: DataFrame = spark.read.csv(args(0)).toDF(schemas:_*)
    userBebaviorDf.createOrReplaceTempView("userBehavior")
    val userItemScoreDf: DataFrame = spark.sql(
      """
        |select
        |    userId,categoryId as itemId,
        |    sum(
        |    case when behaviorType = 'pv' then 1.0
        |    when behaviorType = 'fav' then 2.0
        |    when behaviorType = 'cart' then 3.0
        |    when behaviorType = 'buy' then 4.0
        |    end) as rating
        |from userBehavior
        |group by userId,categoryId
        |order by categoryId,userId
        |limit 200
        |""".stripMargin)

    //所有数据
//    val df: DataFrame = spark.read.json("/Users/user/myproject/spark-base/itemcf_test_data.json")
    val table = "udata"
    userItemScoreDf.createOrReplaceTempView(table)

    //分出训练集和测试集
    val ds: (DataFrame, DataFrame) = dataSet(spark,table)
    ds._1.createOrReplaceTempView("training")
    ds._2.createOrReplaceTempView("test")

    //得到评分数据矩阵
    val matrix: CoordinateMatrix = parseToMatrix(ds._1)

    //计算出物品相似度
    val value: RDD[MatrixEntry] = standardCosine(matrix)

    val simDf: DataFrame = value.toDF()
    simDf.createOrReplaceTempView("itemSim")
    simDf.show()

    val testItemSim = spark.sql(
      """
        |select test.userId,test.itemId,test.rating as testRating,training.rating rating,itemSim.value as sim
        |from test
        |left join training
        |on test.userId=training.userId
        |left join itemSim
        |on test.itemId = itemSim.i and training.itemId = itemSim.j
        |""".stripMargin
    )

    testItemSim.createOrReplaceTempView("testAndSim")
//    testItemSim.cache()
    testItemSim.show()


    val sqlRank = "select userId,itemId,testRating,rating,sim," +
      "rank() over (partition by userId,itemId order by sim desc) rank\n" +
      "from testAndSim"

    val k = 10
    val testAndPre = spark.sql(
      "select userId,itemId,first(testRating) rate,nvl(sum(rating*sim)/sum(abs(sim)),0) pre\n" +
        "from( " +
        "  select *" +
        "  from  (" + sqlRank + ") t " +
        s" where rank <= $k " +
        ") w " +
        "group by userId,itemId order by userId"
    )

    testAndPre.show(100)


    spark.stop()




  }

  //读取评分数据集
  def dataSet(spark:SparkSession,table:String): (DataFrame, DataFrame) = {
//    val table = "udata"
    val df = spark.sql(s"select userId,itemId,rating from $table")
    val Array(training, test) = df.randomSplit(Array(0.8, 0.2))
//    training.cache()
//    test.cache()
    (training, test)
  }

  //计算相似度矩阵
  //评分数据转换成矩阵
  def parseToMatrix(data: DataFrame): CoordinateMatrix = {
    val parsedData = data.rdd.map(item=>{
      val userId: Long = item.getAs[String](0).toLong
      val itemId: Long = item.getAs[String](1).toLong
      val rate: Double = item.getDecimal(2).doubleValue()
      MatrixEntry(userId,itemId,rate)
    })
    new CoordinateMatrix(parsedData)
  }
  //计算相似度矩阵
  def standardCosine(matrix: CoordinateMatrix): RDD[MatrixEntry] = {
    val similarity = matrix
      .toIndexedRowMatrix()
      .columnSimilarities()
    val sim = similarity.entries
    sim
  }
}
