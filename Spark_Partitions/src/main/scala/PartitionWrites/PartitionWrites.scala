package PartitionWrites

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
//import com.databricks.spark.avro._

object PartitionWrites {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Seamless").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val struct = StructType(StructField("txnno", StringType, true) ::
      StructField("txndate", StringType, true) ::
      StructField("custno", StringType, true) ::
      StructField("amount", StringType, true) ::
      StructField("category", StringType, true) ::
      StructField("product", StringType, true) ::
      StructField("state", StringType, true) ::
      StructField("city", StringType, true) ::
      StructField("spendby", StringType, true) :: Nil)
      
    val txns_df = spark.read.format("csv").schema(struct).load("file:///C://Users//abhis//Downloads//sourcefiles//txns")
    
    txns_df.createOrReplaceTempView("partition_df")
    
    txns_df.show()
    
    txns_df.coalesce(1).write.format("csv").partitionBy("category","spendby").mode("overwrite").save("file:///C://Users//abhis//Downloads//sourcefiles//outputfiles//us_partitions")
    
    println("Done")
  }
}