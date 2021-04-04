package Parameterize

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object Parameterize {
  
    def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Seamless").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
  
    val source_format = args(0)
    val source_path = args(1)
    val write_format = args(2)
    val write_mode = args(3)
    val write_path = args(4)
    
    
    val df = spark.read.format(source_format).load(source_path)
    df.write.format(write_format).mode(write_mode).save(write_path)
  }
}