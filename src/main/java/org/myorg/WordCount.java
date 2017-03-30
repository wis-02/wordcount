package org.myorg;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class WordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
/*
        if (args.length < 2) {
            System.err.println("Usage: WordCount <input-dir> <output-dir>");
            System.exit(1);
        }*/

        SparkConf sparkConf = new SparkConf().setAppName("WordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile("C:\\Users\\USER\\Desktop\\lab-spark\\data\\example1\\input");

        JavaRDD<String> words = lines.flatMap(s -> {
            return Arrays.asList(SPACE.split(s)).iterator();
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> {
            return new Tuple2<>(s, 1);
        });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> {
            return i1 + i2;
        });

        counts.saveAsTextFile("C:\\Users\\USER\\Desktop\\lab-spark\\data\\example1\\output");

        sc.stop();

    }

}
