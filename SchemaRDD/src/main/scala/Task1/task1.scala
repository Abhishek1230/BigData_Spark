package Task1

import org.apache.spark._
import org.apache.spark.sql._

object task1 {
  
  case class schema(txnno:Int, txnDate:String, custno:String, amount:String, category:String, product:String, city:String, state:String, transtype:String)
  
  def main(args:Array[String]):Unit ={
    
    val conf = new SparkConf().setAppName("task1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = new SparkSession.Builder().config(conf).getOrCreate()
    import spark.implicits._

    val readtxns = sc.textFile("file:///C:/Users/abhis/Downloads/sourcefiles/txns.txt")
    
    val filter_rdd = readtxns.filter(x => x.contains("cash"))
    
    //val splitdata = filter_rdd.map(x => x.split(",")).map(x => schema(x(0).toInt,x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
    
    val rowdata = filter_rdd.map(x => x.split(",")).map(x => Row(x(0).toInt,x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
    
    val filterrowdata = rowdata.map(x => x(4)=="Gymnastics" | x(4)=="Team Sports" | x(4)=="Exercise & Fitness")
    
//  val category_filter = splitdata.map(x => x.category=="Gymnastics" | x.category=="Team Sports"| x.category=="Exercise & Fitness")
    
    //category_filter.foreach(println)
    
    filterrowdata.foreach(println)
    
  }
}