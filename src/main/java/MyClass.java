import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

public class MyClass {
    public static void main(String[] args) {

//        SparkConf conf = new SparkConf().setAppName("my app").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
//        JavaRDD<Integer> distData = sc.parallelize(data);
//
//        JavaRDD<String> lines = sc.textFile("README.md").map(s -> s.toUpperCase());
//        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
//        int totalLength = lineLengths.reduce((a, b) -> a + b);
//
//        //lineLengths.collect().forEach(p -> System.out.println(p));
//
//        //JavaRDD<String> splitData = lines.map(s -> s.split(" "));
//        JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2(s, 1));
//        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
//
//        counts.collect().forEach(p -> System.out.println(p._1 + " " + Integer.toString(p._2)));


        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                //.master("local")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> df = spark.read().json("sample-lite.json");

// Displays the content of the DataFrame to stdout
        df.show();

        df.printSchema();

        df.select(col("id").as("newId"), col("firstName"),
                when(col("firstName").equalTo("Mario"), "T").otherwise("F"))
                .show();

    }
}
