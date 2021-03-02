package part3typesdatasets;

import Entity.Band;
import Entity.Car;
import Entity.Guitar;
import Entity.GuitarPlayer;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.Tuple2;

import java.sql.Date;
import java.util.ArrayList;

import static org.apache.spark.sql.functions.*;

public class DataSets {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Joins")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> numbersDF = spark.read()
                .format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .csv("src/main/resources/data/numbers.csv");

        // convert dataframe to dataset
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> numberDS = numbersDF.as(integerEncoder);
        Dataset<Integer> transformedDS = numberDS.map(
                (MapFunction<Integer, Integer>) value -> value + 1,
                integerEncoder
        );

        // use filter function
        transformedDS.filter((FilterFunction<Integer>) num -> {
                return num == 705674;
        });
        transformedDS.filter((FilterFunction<Integer>) num -> num == 705674);

        // convert DF to DS
        Encoder<Car> carEncoder = Encoders.bean(Car.class);
        Dataset<Row> carsDF = spark.read()
                //.schema(carEncoder.schema())
                .json("src/main/resources/data/cars.json");
        Dataset<Car> carsDS = carsDF.as(carEncoder);

        carsDF.createOrReplaceTempView("cars");

        Dataset<Row> newCarsDF = spark.sql("SELECT Name FROM cars ");

        Encoder<String> stringEncoder = Encoders.STRING();

        // with DS can use flatmap, map, filter, reduce etc...
        Dataset<String> carsNewDS = carsDS.map(
                (MapFunction<Car, String>) car -> "Name " + car.getName(),
                stringEncoder
        );

        Dataset<String> carsNamesByIndexDF = newCarsDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0).toString(),
                stringEncoder);

        System.out.println(carsDF.where(col("Horsepower").gt(140)).count());

        Long carsCountV1 = carsDS
                .filter((FilterFunction<Car>)  car-> car.getHorsepower() != null && car.getHorsepower() > 140)
                .count();

        carsDS.select(avg(col("Horsepower")).as("AvgHorsepower"));

        carsDS.map((MapFunction<Car, Long>) car -> car.getHorsepower(), Encoders.LONG());

        // joins
        Dataset<Guitar> guitarsDS = readJSON(spark, "guitars")
                .as(Encoders.bean(Guitar.class));
        Dataset<GuitarPlayer> guitarPlayerDS = readJSON(spark, "guitarPlayers")
                .as(Encoders.bean(GuitarPlayer.class));
        Dataset<Band> bandsDS = readJSON(spark, "bands")
                .as(Encoders.bean(Band.class));


        Dataset s = guitarPlayerDS.joinWith(bandsDS, guitarPlayerDS.col("band")
                .equalTo(bandsDS.col("id")))
                .withColumnRenamed("_1", "guitarPlayers");

        carsDS.groupByKey((MapFunction<Car, String>) car -> car.getOrigin(), Encoders.STRING())
                .count()
                .show();






    }




    public static Dataset<Row> readJSON(SparkSession spark, String fileName) {
        return spark.read()
                .option("imferschema", true)
                .json("src/main/resources/data/" + fileName + ".json");
    }


}
