package RDDAPIExamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;


public class InitSparkContext {
    public static JavaSparkContext sc;

    //并行化集合
    @Test
    public void parallelize() {
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);
        System.out.println(distData); //ParallelCollectionRDD[0]

    }
    //外部数据集
    @Test
    public void textFile() {
        JavaRDD<String> distFile = sc.textFile("data.txt");
        System.out.println(distFile); //MapPartitionsRDD[1]
    }
    //持久化
    @Test
    public void persist() {
        JavaRDD<String> lines = sc.textFile("data.txt");
        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        lineLengths.persist(StorageLevel.MEMORY_ONLY());
        int totalLength = lineLengths.reduce((a, b) -> a + b);
        System.out.println(totalLength);
    }
    //回调函数
    @Test
    public void callFunction() {
        JavaRDD<String> lines = sc.textFile("data.txt");
        JavaRDD<Integer> lineLengths = lines.map(new GetLength());
        int totalLength = lineLengths.reduce(new Sum());
        System.out.println(totalLength);
    }
    // RDD 操作 -> map
    @Test
    public void mapReduce() {
        JavaRDD<String> lines = sc.textFile("data.txt");
        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        int totalLength = lineLengths.reduce((a, b) -> a + b);
        System.out.println(totalLength);
    }
    @Test
    public void wordCount() {
        JavaRDD<String> lines = sc.textFile("data.txt");
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2(s, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
        System.out.println(counts.collect());
    }


}

class GetLength implements Function<String, Integer> {
    public Integer call(String s) {
        return s.length();
    }
}

class Sum implements Function2<Integer, Integer, Integer> {
    public Integer call(Integer a, Integer b) {
        return a + b;
    }
}