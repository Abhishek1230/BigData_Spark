/*package readxml

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.databricks.spark.xml
object readxml {
  
    def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Seamless").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val us_data = spark.read.format("xml").option("rowtag","food").load("file:///hdfs://user/cloudera/food.xml")

    
    }
}*/