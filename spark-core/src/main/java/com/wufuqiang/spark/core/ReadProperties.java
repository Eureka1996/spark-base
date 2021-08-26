package com.wufuqiang.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Properties;

public class ReadProperties {
	public static void main(String[] args) throws IOException {


		Properties props = new Properties();
		// 可从resources中读取配置文件
		InputStream resourceAsStream = ReadProperties.class.getClassLoader().getResourceAsStream("hive_where_day.properties");
		props.load(resourceAsStream);
		props.forEach((key,value) ->{
			System.out.println(key+","+value);
		});


		SparkConf sparkConf = new SparkConf().setAppName("SparkWorldCount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> rowRdd = sc.textFile("/Users/user/myproject/spark-base/spark-core/src/main/resources/spark-core-test.data");

		List<String> collect = rowRdd.collect();

		collect.forEach(l -> System.out.println(l));


		sc.stop();
	}
}
