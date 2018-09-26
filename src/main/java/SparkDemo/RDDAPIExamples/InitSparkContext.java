package SparkDemo.RDDAPIExamples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class InitSparkContext {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("appName").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

    }

}
