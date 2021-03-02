package part3typesdatasets;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class CommonTypes {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Joins")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> moviesDF = spark.read()
                .option("inferSchema", "true")
                .json("src/main/resources/data/movies.json");

        moviesDF.select(col("Title"), lit(10).as("MyField"));

        Column dramaFilter = moviesDF.col("Major_Genre").equalTo("Drama");
        Column goodFilms = moviesDF.col("IMDB_Rating").gt(6);
        Column prefferFilter = dramaFilter.and(goodFilms);
        moviesDF.select(col("Title")).where(dramaFilter);
        moviesDF.select(col("Title")).where(prefferFilter);
        Dataset<Row> moviesWithGoodFlagDF = moviesDF.select(col("Title"), prefferFilter.as("GoodFlag"));

        // like where(col("GoodFlag").equal(true))
        moviesWithGoodFlagDF.where(col("GoodFlag"));

        // like where(col("GoodFlag").notEqual(true))
        moviesWithGoodFlagDF.where(not(col("GoodFlag")));

        moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating").divide(10)
                .plus(col("IMDB_Rating"))).divide(2).as("NewRating"));

        // correlation = number between -1 and 1
        System.out.println(moviesDF.stat().corr("Rotten_Tomatoes_Rating", "IMDB_Rating"));

        Dataset<Row> carsDF = spark.read()
                .option("inferSchema", "true")
                .json("src/main/resources/data/cars.json");

        carsDF.select(initcap(col("Name")));

        carsDF.where(col("Name").contains("volkswagen"));

        String regexpString = "volkswagen|vw";
        Dataset<Row> vwDF = carsDF.select(
                col("Name"),
                regexp_extract(col("Name"), regexpString, 0).as("value")
        ).where(col("value").notEqual(""));

        vwDF.select(
                col("Name"),
                regexp_replace(col("Name"), regexpString, "good car")
        );

        moviesDF.where("Major_Genre = 'Drama'");

        // version 1
        List<String> genreList = new ArrayList<>();
        genreList.add("Drama");
        genreList.add("film2");

        String[] genreList2 = {"Drama"};
        moviesDF.where(col("Major_Genre").isin(genreList2));

        moviesDF.where(getWhereConditionGenreList(genreList, moviesDF));

        // version 2
        moviesDF.select(
                col("Title"),
                regexp_extract(col("Title"), getRegexGenreCondition(genreList), 0).as("regex_extract"))
                .where(col("regex_extract").notEqual(""))
                .show();


    }

    public static Column getWhereConditionGenreList(List<String> genreList, Dataset<Row> df) {
        return df.col("Major_Genre").isin(genreList.toArray());
    }

    public static String getRegexGenreCondition(List<String> genreList) {
        return String.join("|", genreList);
    }
}
