package task

import org.apache.spark._
import org.apache.spark.sql._

object task {
  
  def main(args:Array[String]): Unit={
    
    val conf = new SparkConf().setAppName("Flattentest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = new SparkSession.Builder().config(conf).getOrCreate()
    import spark.implicits._
    
    val us_data = sc.textFile("file:///C:/Users/abhis/Downloads/sourcefiles/usdata.csv")
    
    val comma_flat = us_data.flatMap(x => x.split(","))
    
    println
    println("Flatten by comma")
    comma_flat.foreach(println)
    val count1 = comma_flat.count()
    println(count1)
    
    val length_count = us_data.filter(x => x.length()>=180)
    
    val flat_length_comma = length_count.flatMap(x => x.split(","))

    println("Flatten by length>=180")
    
    println
    
    flat_length_comma.foreach(println)
    
    val count2 = flat_length_comma.count()
    println(count2)
    
    val raw3_filter = us_data.filter(x => x.length()<180)
    
    val flatten3 = raw3_filter.flatMap(x => x.split(","))
    
    println("Flatten by length < 180")
    
    println
    
    flatten3.foreach(println)
    
    val count3 = flatten3.count()
    println(count3)
    
  }
}