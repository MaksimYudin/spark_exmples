package part2DataSources;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Aggregation {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Aggregation")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> moviesDF = spark.read()
                .option("inferSchema", "true")
                .json("src/main/resources/data/movies.json");

        // counting, all except null
        moviesDF.select(count(col("Major_Genre")));

        // counting all
        moviesDF.select(count("*"));

        // counting distinct
        moviesDF.select(countDistinct("Major_Genre"));

        // min max
        moviesDF.select(min(col("IMDB_Rating")));
        moviesDF.selectExpr("min(IMDB_Rating)");

        // sum
        moviesDF.select(sum(col("IMDB_Rating")));
        moviesDF.selectExpr("sum(IMDB_Rating)");

        // avg
        moviesDF.select(avg(col("IMDB_Rating")));
        moviesDF.selectExpr("avg(IMDB_Rating)");

        // data science
        moviesDF.select(
                mean(col("Rotten_Tomatoes_Rating")),
                stddev(col("Rotten_Tomatoes_Rating"))
        );

        // grouping
        Dataset<Row> groupByGenreDF = moviesDF.groupBy(col("Major_Genre")).count();
        Dataset<Row> avgRatingByGenre = moviesDF.groupBy(col("Major_Genre"))
                .avg("IMDB_Rating");

        Dataset<Row> aggregationByGenre = moviesDF.groupBy("Major_Genre")
                .agg(
                    count("*").as("N_Movies"),
                    avg("IMDB_Rating").as("Avg_Rating")
                )
                .orderBy(col("Avg_Rating"));

        //avgRatingByGenre.show();

        Dataset<Row> moviesProfitDF = moviesDF.select(
                sum(expr("ifnull(US_Gross, 0) + ifnull(Worldwide_Gross, 0) " +
                        "+ ifnull(US_DVD_Sales, 0) - ifnull(Production_Budget, 0)")).as("Profit")
        );

        Dataset<Row> directorsCountDF = moviesDF.select(countDistinct(col("Director")));
        Dataset<Row> directorsCountDF2 = moviesDF.groupBy(col("Director")).count();
        Dataset<Row> avgRatingGrossByDirectorDF = moviesDF.groupBy(col("Director"))
                .agg(
                  avg(when(col("IMDB_Rating").isNull(), 0).otherwise(col("IMDB_Rating"))).as("RatingAvg"),
                  avg("US_Gross").as("Avg_Gross")
                );
        avgRatingGrossByDirectorDF.show();
    }
}
