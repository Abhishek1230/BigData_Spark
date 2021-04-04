package rowrdd

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object rowrdd {
  
    def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("task1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = new SparkSession.Builder().config(conf).getOrCreate()
    import spark.implicits._

    val readtxns = sc.textFile("file:///C:/Users/abhis/Downloads/sourcefiles/txns.txt")
    
    val row_rdd = readtxns.map(x =>x.split(",")).map(x => Row(x(0).toInt,x(1)))
    
    val schema_struct = StructType(StructField("txnno", IntegerType, true) ::
                                    StructField("txnnodate", StringType, true) :: Nil)
                                    
    //creating DataFrame
    val df = spark.createDataFrame(row_rdd,schema_struct)
    
    df.createOrReplaceTempView("rowrddview")
    
    val sqlquery = spark.sql("select * from rowrddview where txnno<10")
    
    sqlquery.show()
  
  
}
}