package part2DataSources;

import org.apache.spark.internal.config.R;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.WindowSpec;

import javax.xml.crypto.Data;

import static org.apache.spark.sql.expressions.Window.orderBy;
import static org.apache.spark.sql.expressions.Window.partitionBy;
import static org.apache.spark.sql.functions.*;

public class Join {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Joins")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> bandsDF = spark.read()
                .option("inferSchema", "true")
                .json("src/main/resources/data/bands.json");

        Dataset<Row> guitaristsDF = spark.read()
                .option("inferSchema", "true")
                .json("src/main/resources/data/guitarPlayers.json");

        Dataset<Row> guitarsDF = spark.read()
                .option("inferSchema", "true")
                .json("src/main/resources/data/guitars.json");

        Column joinCondition = (guitaristsDF.col("band").equalTo(bandsDF.col("id")));
        Dataset<Row> guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner");

        guitaristsDF.join(bandsDF, joinCondition, "left_outer");

        guitaristsDF.join(bandsDF, joinCondition, "right_outer");

        guitaristsDF.join(bandsDF, joinCondition, "outer");

        guitaristsDF.join(bandsDF, joinCondition, "left_semi");

        guitaristsBandsDF.select(guitaristsDF.col("band"), bandsDF.col("id"));

        // если одинаковые названия полей в DF
        // 1. Переименовать поле по которому соединяем
        guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band");

        // 2. Удалить одно из полей с конфликтным именем
        guitaristsBandsDF.drop(bandsDF.col("id"));

        // 3. Переименовать поле в однои из DF и соединить по нему
        Dataset<Row> bandsModDF = bandsDF.withColumnRenamed("id", "bandId");
        guitaristsDF.join(bandsModDF, guitaristsDF.col("band").equalTo(bandsModDF.col("bandId")));

        // process complex type
        guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"),
                expr("array_contains(guitars, guitarId)"));

        Dataset<Row> employesDF = spark.read()
                .format("jdbc")
                .option("driver", "org.postgresql.Driver")
                .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
                .option("user", "docker")
                .option("password", "docker")
                .option("dbTable", "public.employees")
                .load();

        Dataset<Row> salariesDF = spark.read()
                .format("jdbc")
                .option("driver", "org.postgresql.Driver")
                .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
                .option("user", "docker")
                .option("password", "docker")
                .option("dbTable", "public.salaries")
                .load();

        Dataset<Row> emplManagersDF = spark.read()
                .format("jdbc")
                .option("driver", "org.postgresql.Driver")
                .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
                .option("user", "docker")
                .option("password", "docker")
                .option("dbTable", "public.dept_manager")
                .load();

        Dataset<Row> titlesDF = spark.read()
                .format("jdbc")
                .option("driver", "org.postgresql.Driver")
                .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
                .option("user", "docker")
                .option("password", "docker")
                .option("dbTable", "public.titles")
                .load();

        // 1.
        // max salary by employee
        Dataset<Row> employesWithMaxSalariesDF = employesDF
                .join(salariesDF, employesDF.col("emp_no").equalTo(salariesDF.col("emp_no")))
                .groupBy(employesDF.col("emp_no"))
                .max("salary")
                .orderBy("emp_no");

        // 2.
        Dataset<Row> emplNeverManagersDF = employesDF
                .join(emplManagersDF, employesDF.col("emp_no").equalTo(emplManagersDF.col("emp_no")), "left_outer")
                .where(emplManagersDF.col("emp_no").isNull());

        Dataset<Row> emplNeverManagersDF2 = employesDF
                .join(emplManagersDF, employesDF.col("emp_no").equalTo(emplManagersDF.col("emp_no")), "left_outer")
                .where(emplManagersDF.col("emp_no").isNull())
                .select(employesDF.col("emp_no"));

        Dataset<Row> emplNeverManagersDF3 = employesDF
                .join(emplManagersDF, employesDF.col("emp_no").equalTo(emplManagersDF.col("emp_no")), "left_outer")
                .where(emplManagersDF.col("emp_no").isNull())
                .drop(emplManagersDF.col("emp_no"))
                .limit(10)
                .selectExpr("emp_no");

        Dataset<Row> emplNeverManagersDF4 = employesDF
                .join(emplManagersDF, employesDF.col("emp_no").equalTo(emplManagersDF.col("emp_no")), "left_anti")
                .where(col("emp_no").equalTo(110420))
                .orderBy("emp_no");

        // 3.
        Dataset<Row> best10SalaryEmplDF = salariesDF.groupBy(col("emp_no"))
                .agg(
                        max("salary").as("Max_Salary")
                )
                .orderBy(col("Max_Salary").desc_nulls_last())
                .limit(10);

        WindowSpec spec = partitionBy("emp_no").orderBy(col("to_date").desc());
        Dataset<Row> actualJobTitlesDF = titlesDF
                .withColumn("df1Rank", row_number().over(spec))
                .where(col("df1Rank").equalTo(1));
        //actualJobTitlesDF.show();

        Dataset<Row> jobTitleBestSalaryEmplDF = best10SalaryEmplDF
                .join(actualJobTitlesDF, best10SalaryEmplDF.col("emp_no").equalTo(actualJobTitlesDF.col("emp_no")));

        //jobTitleBestSalaryEmplDF.show();

    }
}
