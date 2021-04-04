package Task4

import org.apache.spark._
import org.apache.spark.sql._

object task4 {

  case class schema(first_name: String, last_name: String, 
      company_name: String, 
      address: String, city: String, 
      county: String, state: String, 
      zip: String, age: String, 
      phone1: String, phone2: String, 
      email:String, web:String)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("task1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = new SparkSession.Builder().config(conf).getOrCreate()
    import spark.implicits._

    val readusdata = sc.textFile("file:///C:/Users/abhis/Downloads/sourcefiles/usdata.csv")
    
    val header = readusdata.first
    
    val remove_header = readusdata.filter(x => !x.contains(header))
    
//  remove_header.foreach(println)
    
    val schemardd = remove_header.map(x => x.split(",")).map(x => schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12))).toDF()
    
    schemardd.createOrReplaceTempView("task4")
    
    val sqlstmt = spark.sql("select * from task4 where state='NJ'")
    
    sqlstmt.show()
  
  
  }
}