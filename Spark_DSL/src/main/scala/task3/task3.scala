package task3

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{ col, expr }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object task3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DSL").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = new SparkSession.Builder().config(conf).getOrCreate()
    import spark.implicits._

    val txns_df = spark.read.format("csv").option("header", "true").load("file:///C:/Users/abhis/Downloads/sourcefiles/coviddata/covid-data.txt")
    
    //txns_df.show()
    
    //.withColumn("txndate", expr("from_unixtime(unix_timestamp(txndate,'MM-dd-yyyy'))"))
    val dsl_df1 = txns_df.withColumn("Date", expr("from_unixtime(unix_timestamp(Date, 'MM-dd-yyyy'))").cast("timestamp"))
                          .withColumn("Current_Match", expr("from_unixtime(unix_timestamp(Current_Match, 'MM-dd-yyyy'))").cast("timestamp"))
  
    dsl_df1.show()
  }
}