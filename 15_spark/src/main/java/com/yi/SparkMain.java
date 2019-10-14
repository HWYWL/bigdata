package com.yi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Spark 测试程序!
 *
 * D:/gitCode/bigdata/15_spark/target/15_spark-1.0-SNAPSHOT.jar
 * spark-submit --class com.yi.SparkMain /gitCode/bigdata/15_spark/target/15_spark-1.0-SNAPSHOT.jar
 *
 * @author huangwenyi
 */
public class SparkMain {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("WorkCount");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> fileRDD = sc.textFile("D:/data/hello.txt");
        JavaRDD<String> wordRdd = fileRDD.flatMap(line -> Arrays.asList(line.split(",")).iterator());

        // 把文件中的每个单词变为一个元组
        JavaPairRDD<String, Integer> pairRDD = wordRdd.mapToPair(word -> new Tuple2<>(word, 1));
        pairRDD.foreach(result -> System.out.println("pairRDD--> 单词：" + result._1 + " 总数：" + result._2));

        // 统计单词的个数
        JavaPairRDD<String, Integer> wordCountRDD = pairRDD.reduceByKey(Integer::sum);
        wordCountRDD.foreach(result -> System.out.println("wordCountRDD--> 单词：" + result._1 + " 总数：" + result._2));

        // 将单词和计数位置调换
        JavaPairRDD<Integer, String> count2WordRDD = wordCountRDD.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
        count2WordRDD.foreach(result -> System.out.println("count2WordRDD--> 总数：" + result._1 + " 单词：" + result._2));

        // 倒叙排序
        JavaPairRDD<Integer, String> sortRDD = count2WordRDD.sortByKey(false);
        sortRDD.foreach(result -> System.out.println("sortRDD--> 总数：" + result._1 + " 单词：" + result._2));

        // 排完序之后再把单词和计数的位置调换
        JavaPairRDD<String, Integer> resultRDD = sortRDD.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
        resultRDD.foreach(result -> System.out.println("resultRDD--> 单词：" + result._1 + " 总数：" + result._2));
        
        // 取前两个数据
        List<Tuple2<String, Integer>> top = resultRDD.take(2);
        JavaRDD<Tuple2<String, Integer>> parallelize = sc.parallelize(top);
        JavaPairRDD<String, Integer> javaRDD = JavaPairRDD.fromJavaRDD(parallelize);
        javaRDD.foreach(result -> System.out.println("javaRDD--> 单词：" + result._1 + " 总数：" + result._2));
    }
}
