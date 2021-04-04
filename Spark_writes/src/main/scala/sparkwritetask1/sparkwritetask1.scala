package sparkwritetask1

import org.apache.spark._
import org.apache.spark.sql._

object sparkwritetask1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Seamless").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    //json doesn't need inferschema as it is a key-value pair the datatypes are already taken as int or string
    val dev_data = spark.read.format("json").load("file:///C://Users//abhis//Downloads//sourcefiles//devices.json")

    dev_data.createOrReplaceTempView("jsondata")
    
    //val filterdata = dev_data.filter($"temp">30)
    
    val filterdata1 = spark.sql("select * from jsondata where temp>30")
    
    dev_data.write.format("parquet").save("file:///C://Users//abhis//Downloads//sourcefiles//outputfiles//parquet_json3")
    
    val parquet_read = spark.read.format("parquet").load("file:///C://Users//abhis//Downloads//sourcefiles//outputfiles//parquet_json3//*")
     
    parquet_read.write.format("csv").option("header","true").save("file:///C://Users//abhis//Downloads//sourcefiles//outputfiles//parquet_csv3")
    
    println("Write done")
  
  }
}