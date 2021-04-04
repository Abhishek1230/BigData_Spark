package joins

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{ col, expr }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object task2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DSL").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = new SparkSession.Builder().config(conf).getOrCreate()
    import spark.implicits._

    val txns_df = spark.read.format("csv").option("header", "true").load("file:///C:/Users/abhis/Downloads/sourcefiles/coviddata/covid-data.txt")
    .groupBy("Direction","Year","Weekday").agg(sum("Value").alias("sum"))
    
    txns_df.show()

  }
}