import org.apache.spark.sql.SparkSession

object spark_test {
  def main(args: Array[String]): Unit = {
    println("Hello Spark_Test with hive support enabled")
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark_test")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "C:\\Users\\Chinmay\\IdeaProjects\\Spark_Aurora_POC\\warehouse_dir")
      .getOrCreate()
    println("Spark created.Loading DF")
    val df = spark.read.option("header", "true").csv("F:\\work\\TCS WORK\\Spark_AWS_POC\\employee_data.csv")
    println("Displaying DF")
    df.show()
    df.createOrReplaceTempView("df")
    spark.sql(
      """
        |select count(*) from df
        |""".stripMargin).show
    spark.sql("Create database if not exists org")
    spark.sql(
      """create table if not exists org.employee as
        |select * from df""".stripMargin)
    spark.sql("show create table org.employee").show(truncate = false)
    spark.sql("select * from org.employee").show(truncate = false)

  }
}
