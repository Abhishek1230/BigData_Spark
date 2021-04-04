package task4

import org.apache.spark._
import org.apache.spark.sql._

object task4 {
  
  def main(args:Array[String]): Unit={
    
    val conf = new SparkConf().setAppName("Flattentest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = new SparkSession.Builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val txns_rdd = sc.textFile("file:///C:/Users/abhis/Downloads/sourcefiles/txns.txt")
    
    val filter_txns = txns_rdd.filter(x => x.contains("Gymnastics"))
    
    println("Filter with Gymnastics")
    
    println
    
    filter_txns.foreach(println)
        
    val us_data = sc.textFile("file:///C:/Users/abhis/Downloads/sourcefiles/usdata.csv")    
  
    val filter_usdata = us_data.filter(x => x.contains("\""))
    
    println("Filter with quotes")
    
    println
    
    filter_usdata.foreach(println)
    
    val union = filter_txns.union(filter_usdata)
    
    union.coalesce(1).saveAsTextFile("file:///C://data//Data//uniondata")
    
    println("Write complete")
    
    
  }
}