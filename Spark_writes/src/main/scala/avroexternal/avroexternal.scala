package avroexternal

import org.apache.spark._
import org.apache.spark.sql._
import com.databricks.spark.avro._

object avroexternal {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Seamless").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    //json doesn't need inferschema as it is a key-value pair the datatypes are already taken as int or string
    val dev_data = spark.read.format("json").load("file:///C://Users//abhis//Downloads//sourcefiles//devices.json")
    
    dev_data.write.format("com.databricks.spark.avro").save("file:///C://Users//abhis//Downloads//sourcefiles//outputfiles//avro_json")
    
    println("Write completed")

  }
}