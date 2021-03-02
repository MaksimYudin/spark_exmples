package part3typesdatasets;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static scala.Predef.Map;

public class ManageNulls {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Joins")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> moviesDF = spark.read()
                .option("inferSchema", "true")
                .json("src/main/resources/data/movies.json");

        moviesDF.select(
                col("Title"),
                col("Rotten_Tomatoes_Rating"),
                col("IMDB_Rating"),
                coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating").multiply(10))
        );

        moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull());

        moviesDF.orderBy(col("Rotten_Tomatoes_Rating").desc_nulls_last());

        // remove rows contains null
        moviesDF.select(col("Title"), col("Rotten_Tomatoes_Rating")).na().drop();

        // replace nulls
        // 1.
        String[] colList = {"Rotten_Tomatoes_Rating", "IMDB_Rating"};
        moviesDF.na().fill(0, colList);

        // 2.
        Map<String, Object> map = new HashMap();
        map.put("Rotten_Tomatoes_Rating", 1000.0);
        map.put("IMDB_Rating", "1000.0");
        moviesDF.na().fill(map);

        moviesDF.selectExpr(
                "Title",
                "Rotten_Tomatoes_Rating",
                "IMDB_Rating",
                "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // like coalesce
                "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // like coalesce
                "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif",// if equals return null, else first value
                "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2"
        ).show();

    }
}
