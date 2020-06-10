package RDDAPIExamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SparkRDDTransformations {
    public static JavaSparkContext sc;
    static JavaRDD<String> lines = null;

    @Before
    public void init(){
        SparkConf conf = new SparkConf().setAppName("appName").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        lines = sc.textFile("data.txt");
    }
    @After
    public void destroy(){
        sc.stop();
    }

    //map , 遍历每一个元素，可对每一个元素做计算  ， k 转为新的 k ,新的k可以为任意类型
    @Test
    public void map() {
        JavaRDD<String> rdd = lines.map(s -> (s.substring(0, 1)));
        System.out.println(rdd.collect());
        JavaRDD<Iterable<String>> mapRDD = lines.map(new MyMapFunction());
        //读取第一个元素
        System.out.println(mapRDD.first());
    }

    //filter  传入自定义函数, 对每一个元素做筛选
    @Test
    public void filter() {
        JavaRDD<String> rdd = lines.filter(new MyFileterFunction());
        System.out.println(rdd.collect());
    }

    // flatMap  ，行转列 Iterator后的每个元素也会被转成一列
    @Test
    public void flatMap() {
        JavaRDD<String> flatMapRDD = lines.flatMap(new MyFlatMapFunction());
        System.out.println(flatMapRDD.collect());
    }

    //mapPartitions  对rdd中的每个分区的迭代器进行操作 Iterator<T> => Iterator<U>
    @Test
    public void mapPartitions() {
        JavaRDD<String> rdd = lines.mapPartitions(new MyMapPartitions());
        System.out.println(rdd.collect());
    }

    // distinct 去重
    @Test
    public void distinct() {
        System.out.println(lines.collect());
        JavaRDD<String> rdd = lines.distinct();
        System.out.println(rdd.collect());
    }

    //union  两RDD的全集
    @Test
    public void union() {
        JavaRDD<String> rdd = lines.union(lines);
        System.out.println(rdd.collect());
    }

    //intersection  两RDD的交集,自动去重
    @Test
    public void intersection() {
        List<String> data = Arrays.asList("1", "2", "111");
        JavaRDD<String> distData = sc.parallelize(data);

        JavaRDD<String> rdd = lines.intersection(distData);
        System.out.println(rdd.collect());
    }

    // join
    @Test
    public void join() {
        List<Integer> data = Arrays.asList(1, 2, 111);
        JavaRDD<Integer> rdd = sc.parallelize(data);
        JavaPairRDD<Integer, Integer> firstRDD = rdd.mapToPair(new MyPairFunction());
        JavaPairRDD<Integer, String> secondRDD = rdd.mapToPair(new MyPairFunction2());
        JavaPairRDD<Integer, Tuple2<Integer, String>> joinRDD = firstRDD.join(secondRDD);
        JavaRDD<String> res = joinRDD.map(new MyJoinFunction());
        System.out.println(res.collect());
    }
    // groupByKey   reduceByKey



}

class MyFileterFunction implements Function<String, Boolean> {
    @Override
    public Boolean call(String s) throws Exception {
        return s.length() > 1;
    }
}

class MyMapFunction implements Function<String, Iterable<String>> {
    @Override
    public Iterable<String> call(String s) throws Exception {
        String[] split = s.split("");
        return Arrays.asList(split);
    }
}

class MyFlatMapFunction implements FlatMapFunction<String, String> {
    @Override
    public Iterator<String> call(String s) throws Exception {
        String[] split = s.split("");
        return Arrays.asList(split).iterator();
    }
}

class MyMapPartitions implements FlatMapFunction<Iterator<String>, String> {
    @Override
    public Iterator<String> call(Iterator<String> stringIterator) throws Exception {
        List<String> list = new ArrayList<String>();
        while (stringIterator.hasNext()) {
            String str = stringIterator.next();
            list.add(str);
        }
        return list.iterator();
    }
}

class MyPairFunction implements PairFunction<Integer, Integer, Integer> {
    @Override
    public Tuple2<Integer, Integer> call(Integer num) throws Exception {
        return new Tuple2<>(num, num * num);
    }
}

class MyPairFunction2 implements PairFunction<Integer, Integer, String> {
    @Override
    public Tuple2<Integer, String> call(Integer num) throws Exception {
        return new Tuple2<>(num, String.valueOf((char) (64 + num * num)));
    }
}

class MyJoinFunction implements Function<Tuple2<Integer, Tuple2<Integer, String>>, String> {
    @Override
    public String call(Tuple2<Integer, Tuple2<Integer, String>> integerTuple2Tuple2) throws Exception {
        int key = integerTuple2Tuple2._1();
        int value1 = integerTuple2Tuple2._2()._1();
        String value2 = integerTuple2Tuple2._2()._2();
        return "<" + key + ",<" + value1 + "," + value2 + ">>";
    }
}