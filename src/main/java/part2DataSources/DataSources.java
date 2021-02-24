package part2DataSources;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;

public class DataSources {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("DataFrame basics")
                .config("spark.master", "local")
                .getOrCreate();

        StructType carsSchema = new StructType(new StructField[]{
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Miles_per_Gallon", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Cylinders", DataTypes.LongType, true, Metadata.empty()),
                new StructField("Displacement", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Horsepower", DataTypes.LongType, true, Metadata.empty()),
                new StructField("Weight_in_lbs", DataTypes.LongType, true, Metadata.empty()),
                new StructField("Acceleration", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Year", DataTypes.DateType, true, Metadata.empty()),
                new StructField("Origin", DataTypes.StringType, true, Metadata.empty()),
        });

        Dataset<Row> carsDF = spark.read()
                .format("json")
                .schema(carsSchema)
                .option("mode", "failFast")
                .option("path", "src/main/resources/data/cars.json")
                .load();

        //carsDF.show();

        Map<String, String> options = new HashMap<>();
        options.put("mode", "failFast");
        options.put("path", "src/main/resources/data/cars.json");
        options.put("inferSchema", "true");

        Dataset<Row> carsDFWithOptionMap = spark.read()
                .format("json")
                .options(options)
                .load();
        carsDFWithOptionMap.show();

        carsDF.write()
                .format("json")
                .mode(SaveMode.Overwrite)
                .option("path", "src/main/resources/data/cars_dupe.json")
                .save();

        // json flag
        spark.read()
                .schema(carsSchema)
                .option("dateFormat", "YYYY-MM-dd")
                .option("allowSingleQuotes", "true")
                .option("compression", "uncompressed")
                .json("src/main/resources/data/cars.json");

        // csv flags
        StructType stocksSchema = new StructType(new StructField[]{
                new StructField("symbol", DataTypes.StringType, true, Metadata.empty()),
                new StructField("date", DataTypes.DateType, true, Metadata.empty()),
                new StructField("price", DataTypes.DoubleType, true, Metadata.empty())
        });

        // csv
        spark.read()
                .format("csv")
                .schema(stocksSchema)
                .option("dateFormat", "MMM dd YYYY")
                .option("header", "true")
                .option("nullValue", "")
                .csv("src/main/resources/data/stocks.csv");

        // write parquet
        carsDF.write()
                .mode(SaveMode.Overwrite)
                .save("src/main/resources/data/cars.parquet");

        // text
        spark.read().textFile("src/main/resources/data/sample_text.txt").show();

        // postgres
        Dataset<Row> employesDF = spark.read()
                .format("jdbc")
                .option("driver", "org.postgresql.Driver")
                .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
                .option("user", "docker")
                .option("password", "docker")
                .option("dbTable", "public.employees")
                .load();

        employesDF.write()
                .format("jdbc")
                .option("driver", "org.postgresql.Driver")
                .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
                .option("user", "docker")
                .option("password", "docker")
                .option("dbTable", "public.employees_new")
                .save();
    }


}
