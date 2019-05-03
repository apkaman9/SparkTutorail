package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.spark_project.guava.collect.Iterables;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {

		List<String> inputData=new ArrayList<>();
		
		inputData.add("WARN: Tuesday 4 September 0405");
		inputData.add("ERROR: Tuesday 4 September 0408");
		inputData.add("FATAL: Wednesday 05 September 1632");
		inputData.add("ERROR: Friday 07 September 1854");
		inputData.add("WARN: Satueday 08 September 1942");
		
		org.apache.log4j.Logger.getLogger("org.apche").setLevel(org.apache.log4j.Level.WARN);
		
		//Logger.getLogger("org.apache").setLevel(Level.WARNING);
		
		SparkConf conf=new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
		
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		JavaRDD<String> sentences=sc.parallelize(inputData);
		JavaRDD<String> words=sentences.flatMap(value->Arrays.asList(value.split(" ")).iterator());
		JavaRDD<String> filterWords=words.filter(word->word.length()>1);
		filterWords.foreach(data->System.out.println(data));
		
		sc.close();

	}
	
	public void flatMap() {

		List<String> inputData=new ArrayList<>();
		
		inputData.add("WARN: Tuesday 04 September 0405");
		inputData.add("ERROR: Tuesday 04 September 0408");
		inputData.add("FATAL: Wednesday 05 September 1632");
		inputData.add("ERROR: Friday 07 September 1854");
		inputData.add("WARN: Satueday 08 September 1942");
		
		org.apache.log4j.Logger.getLogger("org.apche").setLevel(org.apache.log4j.Level.WARN);
		
		//Logger.getLogger("org.apache").setLevel(Level.WARNING);
		
		SparkConf conf=new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
		
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		JavaRDD<String> sentences=sc.parallelize(inputData);
		JavaRDD<String> words=sentences.flatMap(value->Arrays.asList(value.split(" ")).iterator());
		words.foreach(data->System.out.println(data));
		
		sc.close();

	}
	
	public void groupByKey() {

		List<String> inputData=new ArrayList<>();
		
		inputData.add("WARN: Tuesday 04 September 0405");
		inputData.add("ERROR: Tuesday 04 September 0408");
		inputData.add("FATAL: Wednesday 05 September 1632");
		inputData.add("ERROR: Friday 07 September 1854");
		inputData.add("WARN: Satueday 08 September 1942");
		
		org.apache.log4j.Logger.getLogger("org.apche").setLevel(org.apache.log4j.Level.WARN);
		
		//Logger.getLogger("org.apache").setLevel(Level.WARNING);
		
		SparkConf conf=new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
		
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		/*
		 * sc.parallelize(inputData) .mapToPair(keyValue->new Tuple2<String,
		 * Long>(keyValue.split(":")[0], 1L))
		 * .reduceByKey((value1,value2)->value1+value2)
		 * .foreach(tuple->System.out.println(tuple._1 + " has "+ tuple._2
		 * +"instances"));
		 */
		
		//groupByKey
		
		sc.parallelize(inputData)
		.mapToPair(keyValue->new Tuple2<String, Long>(keyValue.split(":")[0], 1L))
		.groupByKey()
		.foreach(tuple->System.out.println(tuple._1 + " has "+ Iterables.size(tuple._2) +" instances"));
		
		sc.close();

	}
	
	public void fluentApi() {

		List<String> inputData=new ArrayList<>();
		
		inputData.add("WARN: Tuesday 04 September 0405");
		inputData.add("ERROR: Tuesday 04 September 0408");
		inputData.add("FATAL: Wednesday 05 September 1632");
		inputData.add("ERROR: Friday 07 September 1854");
		inputData.add("WARN: Satueday 08 September 1942");
		
		org.apache.log4j.Logger.getLogger("org.apche").setLevel(org.apache.log4j.Level.WARN);
		
		//Logger.getLogger("org.apache").setLevel(Level.WARNING);
		
		SparkConf conf=new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
		
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		sc.parallelize(inputData)
		.mapToPair(keyValue->new Tuple2<String, Long>(keyValue.split(":")[0], 1L))
		.reduceByKey((value1,value2)->value1+value2)
		.foreach(tuple->System.out.println(tuple._1 + " has "+ tuple._2 +"instances"))		
				;
		sc.close();

	}
	
	public void reduceByKey() {


		List<String> inputData=new ArrayList<>();
		
		inputData.add("WARN: Tuesday 04 September 0405");
		inputData.add("ERROR: Tuesday 04 September 0408");
		inputData.add("FATAL: Wednesday 05 September 1632");
		inputData.add("ERROR: Friday 07 September 1854");
		inputData.add("WARN: Satueday 08 September 1942");
		
		org.apache.log4j.Logger.getLogger("org.apche").setLevel(org.apache.log4j.Level.WARN);
		
		//Logger.getLogger("org.apache").setLevel(Level.WARNING);
		
		SparkConf conf=new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
		
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		JavaRDD<String> originalLogMessages=sc.parallelize(inputData);
		
		JavaPairRDD<String, Long> pairRdd=originalLogMessages.mapToPair(keyValue->{
			String[] cols=keyValue.split(":");
			String level=cols[0];
			return new Tuple2<String, Long>(level, 1L);
		});
		JavaPairRDD<String, Long> sumRDD=pairRdd.reduceByKey((value1,value2)->value1+value2);
		sumRDD.foreach(tuple->System.out.println(tuple._1 + " has "+ tuple._2 +"instances"));
		sc.close();

	
	}
	
	
	public void pairRDD() {


		List<String> inputData=new ArrayList<>();
		
		inputData.add("WARN: Tuesday 04 September 0405");
		inputData.add("ERROR: Tuesday 04 September 0408");
		inputData.add("FATAL: Wednesday 05 September 1632");
		inputData.add("ERROR: Friday 07 September 1854");
		inputData.add("WARN: Satueday 08 September 1942");
		
		org.apache.log4j.Logger.getLogger("org.apche").setLevel(org.apache.log4j.Level.WARN);
		
		//Logger.getLogger("org.apache").setLevel(Level.WARNING);
		
		SparkConf conf=new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
		
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		JavaRDD<String> originalLogMessages=sc.parallelize(inputData);
		
		JavaPairRDD<String, String> pairRdd=originalLogMessages.mapToPair(keyValue->{
			String[] cols=keyValue.split(":");
			String level=cols[0];
			String time=cols[1];
			return new Tuple2<String, String>(level, time);
		});
		
	
		sc.close();

	
	}
	
	public void tuple2() {

		List<Integer> inputData=new ArrayList<>();
		
		inputData.add(2);
		inputData.add(589);
		inputData.add(302);
		inputData.add(2287789);
		org.apache.log4j.Logger.getLogger("org.apche").setLevel(org.apache.log4j.Level.WARN);
		
		//Logger.getLogger("org.apache").setLevel(Level.WARNING);
		
		SparkConf conf=new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
		
		JavaSparkContext sc=new JavaSparkContext(conf);
		JavaRDD<Integer> myRDD=sc.parallelize(inputData);
		JavaRDD<Tuple2<Integer, Double>> sqrtRDD=myRDD.map(value-> new Tuple2(value, Math.sqrt(value)));
	
		sc.close();

	
	}
	public void main2() {
		List<Integer> inputData=new ArrayList<>();
		
		inputData.add(2);
		inputData.add(589);
		inputData.add(302);
		inputData.add(2287789);
		org.apache.log4j.Logger.getLogger("org.apche").setLevel(org.apache.log4j.Level.WARN);
		
		//Logger.getLogger("org.apache").setLevel(Level.WARNING);
		
		SparkConf conf=new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
		
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		JavaRDD<Integer> myRDD=sc.parallelize(inputData);
		Integer sum=myRDD.reduce((value1,value2)->value1+value2);
		
		JavaRDD<Double> sqrtRdd= myRDD.map(value->Math.sqrt(value));
		
		sqrtRdd.foreach(value->System.out.println(value));
		
		System.out.println("Using Count method "+sqrtRdd.count());
		
		JavaRDD<Long> countRDD=sqrtRdd.map(value -> 1L);
		
		
		Long count=countRDD.reduce((val1,val2)-> val1+val2);
		
		System.out.println("Using count RDD:"+count);
		
		
		System.out.println("Sum="+sum);
		
		sqrtRdd.collect().forEach(System.out::println);
		sc.close();

	}

}
