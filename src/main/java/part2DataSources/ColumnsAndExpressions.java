package part2DataSources;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import static org.apache.spark.sql.functions.*;

public class ColumnsAndExpressions {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("DataFrame basics")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> carsDF = spark.read()
                .format("json")
                .option("inferSchema", "true")
                .option("mode", "failFast")
                .option("path", "src/main/resources/data/cars.json")
                .load();

        // read column
        Column name = new Column("name");
        Dataset<Row> carsNameDF = carsDF.select(name);
        //carsNameDF.show();

        // read columns
        carsDF.select(
                carsDF.col("Name"),
                col("Miles_per_Gallon"),
                column("Cylinders"),
                expr("origin")
        ).show();

        carsDF.select("Name", "Miles_per_Gallon").show();

        // Expressions
        Column weightLbs = carsDF.col("Weight_in_lbs");
        Column weightKg = carsDF.col("Weight_in_lbs").divide(2.2).as("Weight_in_Kg");
        carsDF.select(
                col("Name"),
                col("Weight_in_lbs"),
                weightKg,
                expr("Weight_in_lbs / 2.2").as("Weight_in_Kg_2")
        );

        // selectExpr
        carsDF.selectExpr(
                "Name",
                "Weight_in_lbs",
                "Weight_in_lbs / 2.2 as Weight_in_kg"
        );

        // DF processing
        // add new field
        Dataset<Row> carsWeightKgDF = carsDF.withColumn("weight_in_kg", col("Weight_in_lbs").divide(2.2));
        //carsWeightKgDF.show();

        // add list of fields
        List<String> columnNames = new ArrayList<>();
        columnNames.add("Name1");
        columnNames.add("Weight_in_lbs1");

        List<Column> columns = new ArrayList<>();
        columns.add(carsDF.col("Name"));
        columns.add(carsDF.col("Weight_in_lbs"));

        Seq seqColNames = JavaConverters.asScalaIteratorConverter(columnNames.iterator()).asScala().toSeq();
        Seq seqColumns = JavaConverters.asScalaIteratorConverter(columns.iterator()).asScala().toSeq();

        Dataset<Row> carsWithWeightKgDF = carsDF.withColumns(seqColNames, seqColumns);
        //carsWithWeightKgDF.show();

        // rename a column
        Dataset<Row> carWithRenamedColumn = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in lbs");

        // remove column
        carWithRenamedColumn.drop("Name");

        // filtering
        Dataset<Row> europeanCars1 = carsDF.filter(col("Origin").equalTo("Europe"));
        Dataset<Row> europeanCars2 = carsDF.where(col("Origin").notEqual("USA"));
        Dataset<Row> europeanCars3 = carsDF.filter("Origin = 'Europe'");

        // multiple filter
        Dataset<Row> americanPowerfulCars1 = carsDF.filter(col("Origin").equalTo("USA"))
                .filter(col("Horsepower").$greater(150));
        Dataset<Row> americanPowerfulCars2 = carsDF.where(
                (col("Origin").equalTo("USA")).and(col("Horsepower").$greater(150)));
        Dataset<Row> americanPowerfulCars3 = carsDF.where("Origin = 'USA' and Horsepower > 150");
        //americanPowerfulCars3.show();

        // union
        Dataset<Row> moreCarsDF = spark.read()
                .format("json")
                .option("inferSchema", "true")
                .option("mode", "failFast")
                .option("path", "src/main/resources/data/more_cars.json")
                .load();

        Dataset<Row> allCars = carsDF.union(moreCarsDF);

        // distinct
        Dataset<Row> allCountries = allCars.select("Origin").distinct();
        //allCountries.show();

        // exercises
        Dataset<Row> moviesDF = spark.read()
                .option("inferSchema", "true")
                .json("src/main/resources/data/movies.json");

        Dataset<Row> moviesInfoDF = moviesDF.select(col("Title"), col("Release_Date"));

        Dataset<Row> moviesProfitDF = moviesDF.select(
                col("Title"),
                expr("ifnull(US_Gross, 0) + ifnull(Worldwide_Gross, 0) + ifnull(US_DVD_Sales, 0)").as("Total_sum"),
                (col("US_Gross").plus(
                        col("Worldwide_Gross")).plus(
                                when(col("US_DVD_Sales").isNull(), 0)
                                .otherwise(col("US_DVD_Sales"))
                )).as("Total_sum2")
        );
        moviesProfitDF.show();

        Dataset<Row> moviesProfitDF2 = moviesDF.selectExpr(
                "Title",
                "ifnull(US_Gross, 0) + ifnull(Worldwide_Gross, 0) + ifnull(US_DVD_Sales, 0) as Total_sum"
        );
        //moviesProfitDF2.show();
        Dataset<Row> allComedyWithHighRatingDF = moviesDF
                .where(lower(col("Major_Genre")).like("%comedy%")
                .and(col("IMDB_Rating").$greater(6))
                );
        //allComedyWithHighRatingDF.show();

    }
}
