package schemardd

import org.apache.spark._
import org.apache.spark.sql._

object schemardd {
  
  case class schema(txnno:Int, txnDate:String, custno:String, amount:String, category:String, product:String, city:String, state:String, transtype:String)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Flattentest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = new SparkSession.Builder().config(conf).getOrCreate()
    import spark.implicits._

    val txns_rdd = sc.textFile("file:///C:/Users/abhis/Downloads/sourcefiles/txns.txt")
    
    val gym_data = txns_rdd.filter(x => x.contains("Gymnastics"))
    
    val splitdata = gym_data.map(x => x.split(",")).map(x => schema(x(0).toInt,x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8))).toDF()
    
    splitdata.createOrReplaceTempView("Rowview")
    
    val cashdata = spark.sql("select * from Rowview where transtype = 'cash'")
    
    cashdata.show()
    //val filterdata = splitdata.filter(x => x.txnno>50000 & x.transtype=="cash")
    
    //filterdata.foreach(println)

  }
}