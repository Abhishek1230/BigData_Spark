package Spark_write

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object Spark_write {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Seamless").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val dev_data = spark.read.format("json").option("inferSchema", "true").load("file:///C://Users//abhis//Downloads//sourcefiles//devices.json")

    val filter_data = dev_data.filter($"temp" > 20)

    println("temp_data print")
    filter_data.show()
    filter_data.printSchema()

    //dev_data.write.format("csv").option("header","true").option("delimiter","|").save("file:///C:/Users/abhis/Downloads/sourcefiles/outputfiles/json_csv1")
    dev_data.write.format("parquet").save("file:///C:/Users/abhis/Downloads/sourcefiles/outputfiles/parquet_json")
    println("written done")

  }
}