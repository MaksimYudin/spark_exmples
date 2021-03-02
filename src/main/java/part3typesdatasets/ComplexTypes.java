package part3typesdatasets;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.spark.sql.functions.*;

public class ComplexTypes {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Joins")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> moviesDF = spark.read()
                .option("inferSchema", "true")
                .json("src/main/resources/data/movies.json");

        moviesDF
                .select(col("Title"), col("Release_Date"),
                to_date(col("Release_Date"), "d-MMM-yyyy").as("Actual_Release"))
                .withColumn("Today", current_date())
                .withColumn("Now", current_timestamp());

        // 1.
        List<Column> columnSet = new ArrayList<>();//{col("US_Gross"), col("Worldwide_Gross")};
        moviesDF
                .select(col("Title"),
                        struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
                .select(col("Title"), col("Profit").getField("US_Gross").as("US_Gross"));

        // 2.
        moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
            .selectExpr("Title", "Profit.US_Gross as US_Gross");

        // Array
        Dataset<Row> moviesWithWordsDF = moviesDF.select(col("Title"),
                split(col("Title"), " |, ").as("Title_Words"));

        moviesWithWordsDF.select(
                col("Title"),
                expr("Title_Words[0] as First_Word"),
                size(col("Title_Words")).as("Size"),
                array_contains(col("Title_Words"), "Love")
        ).show();
    }
}
