import org.apache.spark.sql.SparkSession
import java.util.Properties

/*
Starting code to connect to aurora MYSQL DB
 */

object mysql_test {
  def main(args: Array[String]): Unit = {
    println("Hello MYSQL_TEST")
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark_test")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "C:\\Users\\Chinmay\\IdeaProjects\\Spark_Aurora_POC\\warehouse_dir")
      .getOrCreate()
    println("Spark created. Below is the hive table data.")
    spark.sql("select * from org.employee").show(truncate = false)
    println("Starting to connect Aurora MYSQL DB.")

    val url = "jdbc:mysql://database-1-instance-1.cvcxdv65fgyj.ap-south-1.rds.amazonaws.com/chinmay_testdb"
    val driver = "com.mysql.jdbc.Driver"
    val username = "admin"
    val password = "chinmay8"

    val connectionProperties = new Properties()
    connectionProperties.put("user", username)
    connectionProperties.put("password", password)
    connectionProperties.setProperty("Driver", driver)
    val df = spark.read.jdbc(url=url, table="chinmay_testdb.employee", properties=connectionProperties)
    println("Displaying MYSQL table data.")
    df.show(truncate = false)
    println("Displaying MYSQL table data greater than 50K salary.")
    df.filter(df("salary") > 50000).show()
  }

}
