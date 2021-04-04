package task4

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object doublequote {
  
    def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Seamless").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val us_data = sc.textFile("file:///C:/Users/abhis/Downloads/sourcefiles/usdata.csv")

//    val struct_schema = StructType(StructField("firstname", StringType, true) ::
//      StructField("lastname1", StringType, true) ::
//      StructField("company1", StringType, true) ::
//      StructField("address1", StringType, true) ::
//      StructField("city1", StringType, true) ::
//      StructField("zip1", StringType, true) ::
//      StructField("age1", StringType, true) ::
//      StructField("phone1", StringType, true) ::
//      StructField("phone2", StringType, true) ::
//      StructField("email1", StringType, true) ::
//      StructField("web", StringType, true) :: Nil)
      
    val row_rdd = us_data.map(x => x.split(",")).map(x => Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10)))
    
    row_rdd.foreach(println)
    
  
    
//    val df = spark.createDataFrame(row_rdd, struct_schema)
    
//    df.show()
    
      
      
}
}