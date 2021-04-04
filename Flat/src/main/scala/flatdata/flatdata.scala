package flatdata

import org.apache.spark._
import org.apache.spark.sql._

object flatdata {

  
  def main(args:Array[String]): Unit={
    
    val conf = new SparkConf().setAppName("Flatten").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
   
    val spark = new SparkSession.Builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val us_data = sc.textFile("file:///C:/Users/abhis/Downloads/sourcefiles/usdata.csv")
    
    val len_data = us_data.filter(x => x.length()>190)
    
    println("Length lines greater than 190")
    len_data.foreach(println)
    
    println
    val flat_data = len_data.flatMap(x => x.split(","))
    
    println("flatten data")
    flat_data.foreach(println)
    
    val flat_data_space = flat_data.flatMap(x => x.split(" "))
   
    println
    println("Flatten space data")
    flat_data_space.foreach(println)
    
    val flat_quotes = flat_data_space.filter(x => x.contains("\""))

    println
    println("Flatten with quotes")
    flat_quotes.foreach(println)

    flat_quotes.coalesce(1).saveAsTextFile("file:///C://data//Data//quote_data1//")
    
    println("Write complete")
    
  }
  
}
