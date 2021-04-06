package Complexdata

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object randomuser {
  
    def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DSL").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = new SparkSession.Builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val random_user = spark.read.format("json").option("multiLine","true").load("file:///C:/Users/abhis/Downloads/sourcefiles//random_user.json")
    random_user.printSchema()
    
    val df_read = random_user.withColumn("results",explode(col("results"))).select("results.user.gender","results.user.name.title","results.user.name.first","results.user.name.last","results.user.location.street",      
        "results.user.location.city","results.user.location.state","results.user.location.zip","results.user.email","results.user.username","results.user.password","results.user.salt","results.user.md5","results.user.sha1","results.user.sha256","results.user.registered","results.user.dob","results.user.phone","results.user.cell","results.user.SSN","results.user.picture.large","results.user.picture.medium","results.user.picture.thumbnail","nationality","seed", "version")
    df_read.printSchema()
    df_read.show()
    
    df_read.write.format("json").option("header","true").save("file:///C:/Users/abhis/Downloads/outputfiles/Explodejson_output")
    
    df_read.write.format("com.databricks.spark.avro").save("file:///C:/Users/abhis/Downloads/outputfiles/Explodeavro_output")
    
    println("Write completed")
    
  }
}