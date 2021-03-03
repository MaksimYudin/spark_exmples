package part4sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class SparkSQL {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SQL")
                .config("spark.master", "local")
                //.config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
                .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
                .getOrCreate();

        Dataset<Row> carsDF = spark.read()
                .option("inferSchema", true)
                .json("src/main/resources/data/cars.json");

        Dataset<Row> americanCarsDF = carsDF.where(col("Origin").equalTo("USA"));
        americanCarsDF.createOrReplaceTempView("cars");

        spark.sql("""
                    select *
                    from cars""");

        spark.sql("create database rtjvm");
        spark.sql("use rtjvm");

        Dataset<Row> databaseDF = spark.sql("show databases");

        Dataset<Row> employesDF = spark.read()
                .format("jdbc")
                .option("driver", "org.postgresql.Driver")
                .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
                .option("user", "docker")
                .option("password", "docker")
                .option("dbTable", "public.employees")
                .load();

        //spark.sql("drop table if exists employees");

        employesDF.write()
                .mode(SaveMode.Overwrite)
                .option("path", "src/main/resources/warehouse")
                .saveAsTable("employees");

        Dataset<Row> newEmployeeDF = spark.read().table("employees");

        // 1
        Dataset<Row> hiredEmployeesDS = spark.sql("""
            select * 
            from employees
            where hire_date between '1999-01-01' and '2000-01-01'
        """);

        Dataset<Row> ds = spark.sql("""
SELECT de.dept_no as dept_no,
		avg(COALESCE(s.salary, 0)) as avg_salary
	FROM employees as e
	join dept_emp as de
		on e.emp_no = de.emp_no
			and e.hire_date BETWEEN de.from_date and de.to_date
	join salaries as s
		on e.emp_no = s.emp_no
			and e.hire_date BETWEEN s.from_date and s.to_date
	where e.hire_date BETWEEN '1999-01-01' and '2000-01-01'
	group by de.dept_no
""");
        ds.show();
    }
}
