package Task5

import org.apache.spark._
import org.apache.spark.sql._

object task5 {

  case class schema(txnno: Int, spendby: String)
  
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("task1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = new SparkSession.Builder().config(conf).getOrCreate()
    import spark.implicits._

    val readtxns = sc.textFile("file:///C:/Users/abhis/Downloads/sourcefiles/txns.txt")
    
    val splitdata = readtxns.map(x => x.split(",")).map(x => schema(x(0).toInt,x(8))).toDF()
    
    splitdata.createOrReplaceTempView("twocolumns")
    
    val query = spark.sql("select * from twocolumns where txnno<10")
    
    query.show()
    
    

  }
}