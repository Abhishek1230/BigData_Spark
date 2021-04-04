package task5

import org.apache.spark._
import org.apache.spark.sql._

object task5 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Flattentest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = new SparkSession.Builder().config(conf).getOrCreate()
    import spark.implicits._

    val txns_rdd = sc.textFile("file:///C:/Users/abhis/Downloads/sourcefiles/txns.txt")

    val filter_txns = txns_rdd.filter(x => x.contains("Gymnastics"))
    
    //val splitdata = filter_txns.map(x => x.split(",")).map(x => x(3) + " " + x(1) + "~" + x(8))

    //splitdata.take(20).foreach(println)
    
    val count_gym = filter_txns.count()
    
    println(count_gym)
    
    val union_gym = filter_txns.union(filter_txns)
    
    val count_union = union_gym.count()
    
    println(count_union)
    
    val distinct_gymdata = union_gym.distinct()
    
    val count_distinct = distinct_gymdata.count()
    
    println(count_distinct)
    
  }
  
}