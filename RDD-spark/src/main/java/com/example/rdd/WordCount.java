package com.example.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        String inputFile = args.length > 0 ? args[0] : "hdfs://namenode:9000/input/";
        String outputFile = args.length > 1 ? args[1] : "hdfs://namenode:9000/output/result_" + System.currentTimeMillis();

        System.out.println("=========================================");
        System.out.println("Processing input: " + inputFile);
        System.out.println("Saving output to: " + outputFile);
        System.out.println("=========================================");

        SparkConf conf = new SparkConf().setAppName("WordCount-Distributed");
        JavaSparkContext sc = new JavaSparkContext(conf);

        long t1 = System.currentTimeMillis();
        JavaRDD<String> data = sc.textFile(inputFile).flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> counts = data.mapToPair(w -> new Tuple2<>(w, 1))
                .reduceByKey((c1, c2) -> c1 + c2);
        counts.saveAsTextFile(outputFile);
        long t2 = System.currentTimeMillis();

        System.out.println("=========================================");
        System.out.println("SUCCESS! Execution time: " + (t2 - t1) + " ms");
        System.out.println("=========================================");

        sc.stop();
    }
}