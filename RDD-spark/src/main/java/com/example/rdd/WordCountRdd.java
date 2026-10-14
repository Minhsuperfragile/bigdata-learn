package com.example.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * WordCount using Apache Spark RDD API in Java.
 * Optimized for low memory (< 512 MB) and zero big data dependencies.
 */
public class WordCountRdd {

    public static void main(String[] args) {
        // Bypass Spark's 471MB minimum memory guard via JVM system property
        System.setProperty("spark.testing.memory", "471859200");

        // 1. Configure Spark for low-memory local execution (< 512 MB)
        SparkConf conf = new SparkConf()
                .setAppName("RDD-WordCount-Learning")
                .setMaster("local[*]")                  // Run locally on available CPU cores
                .set("spark.driver.memory", "512m")     // 512MB max heap
                .set("spark.executor.memory", "512m")
                .set("spark.ui.enabled", "false");      // Disable web UI to save memory & ports

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            System.out.println("==================================================");
            System.out.println(" Apache Spark RDD Demo (Java 21 / Low Memory)");
            System.out.println("==================================================");

            // 2. Sample data in memory (no external file needed for quick testing)
            List<String> sentences = Arrays.asList(
                    "Hello Apache Spark",
                    "Spark uses Resilient Distributed Datasets called RDD",
                    "RDD is the fundamental data abstraction in Spark",
                    "Hello Big Data World and Hello Spark"
            );

            // 3. Create an RDD from the in-memory collection (Parallelized Collection)
            JavaRDD<String> linesRdd = sc.parallelize(sentences, 2);
            System.out.println("\n[Action] Total input lines: " + linesRdd.count());

            // 4. Transformation: flatMap lines -> words
            JavaRDD<String> wordsRdd = linesRdd.flatMap(line ->
                    Arrays.asList(line.toLowerCase().split("\\s+")).iterator()
            );

            // 5. Transformation: filter out small words (< 3 chars)
            JavaRDD<String> filteredWordsRdd = wordsRdd.filter(word -> word.length() >= 3);

            // 6. Transformation: map words to Pair RDD (word, 1)
            JavaPairRDD<String, Integer> wordPairsRdd = filteredWordsRdd.mapToPair(word ->
                    new Tuple2<>(word, 1)
            );

            // 7. Transformation: reduceByKey to count occurrences
            JavaPairRDD<String, Integer> wordCountsRdd = wordPairsRdd.reduceByKey(Integer::sum);

            // 8. Action: collect results back to the driver program
            List<Tuple2<String, Integer>> results = wordCountsRdd.collect();

            // 9. Display the word counts
            System.out.println("\n--- Word Count Results (Filtered len >= 3) ---");
            results.forEach(tuple ->
                    System.out.printf("%-15s : %d%n", tuple._1, tuple._2)
            );
            System.out.println("----------------------------------------------");

            // 10. Optional: Reading from Docker HDFS
            // If you have uploaded a file into HDFS (e.g. hdfs://localhost:9000/input/sample.txt):
            /*
            try {
                JavaRDD<String> hdfsLines = sc.textFile("hdfs://localhost:9000/input/sample.txt");
                System.out.println("HDFS file line count: " + hdfsLines.count());
            } catch (Exception e) {
                System.out.println("HDFS file not found or cluster offline. Skipping HDFS read.");
            }
            */

            System.out.println("\n[SUCCESS] RDD pipeline completed successfully with low memory!");
        }
    }
}
