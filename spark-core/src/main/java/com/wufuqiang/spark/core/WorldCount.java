package com.wufuqiang.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class WorldCount {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("SparkWorldCount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> rowRdd = sc.textFile("/Users/user/myproject/spark-base/spark-core/src/main/resources/spark-core-test.data");

		List<String> collect = rowRdd.collect();

		collect.forEach(l -> System.out.println(l));


		sc.stop();
	}
}
