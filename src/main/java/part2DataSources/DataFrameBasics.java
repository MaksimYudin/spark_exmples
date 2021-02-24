package part2DataSources;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataFrameBasics {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("DataFrame basics")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("json")
                .option("inferSchema", "true")
                .load("src/main/resources/data/cars.json");
                //.toDF();

        df.show();
        df.printSchema();

        for (Row r : (Row[]) df.take(10)) {
            System.out.println(r);
        }

        DataType longType = DataTypes.LongType;

        //DataType stringType = DataType.fromJson();
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("Miles_per_Gallon", DataTypes.DoubleType, true));
        structFields.add(DataTypes.createStructField("Cylinders", DataTypes.LongType, true));
        structFields.add(DataTypes.createStructField("Displacement", DataTypes.DoubleType, true));
        structFields.add(DataTypes.createStructField("Horsepower", DataTypes.LongType, true));
        structFields.add(DataTypes.createStructField("Weight_in_lbs", DataTypes.LongType, true));
        structFields.add(DataTypes.createStructField("Acceleration", DataTypes.DoubleType, true));
        structFields.add(DataTypes.createStructField("Year", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("Origin", DataTypes.StringType, true));
        StructType carsSchema = new StructType(new StructField[]{
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Miles_per_Gallon", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Cylinders", DataTypes.LongType, true, Metadata.empty()),
                new StructField("Displacement", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Horsepower", DataTypes.LongType, true, Metadata.empty()),
                new StructField("Weight_in_lbs", DataTypes.LongType, true, Metadata.empty()),
                new StructField("Acceleration", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Year", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Origin", DataTypes.StringType, true, Metadata.empty()),
        });

        StructType schema = new StructType(new StructField[]{
                new StructField("feature", DataTypes.DoubleType, false, Metadata.empty())
        });

        Row myRow = RowFactory.create("aaa", schema);

        List<Row> carRows = new ArrayList<>();
        carRows.add(RowFactory.create("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"));
        carRows.add(RowFactory.create("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"));

        Dataset<Row> dfCars = spark.createDataFrame(Arrays.asList(
                RowFactory.create("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
                RowFactory.create("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA")
        ), carsSchema);
        dfCars.show();

        Dataset<Row> dfCars2 = spark.createDataFrame(carRows, carsSchema);
        dfCars2.show();

       Dataset<Row> dfCarsImplicit = dfCars2.toDF("f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9");

       dfCars2.printSchema();
       dfCarsImplicit.printSchema();

       System.out.println(dfCars2.count());

    }




}
