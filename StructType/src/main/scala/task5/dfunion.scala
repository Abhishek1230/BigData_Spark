package task5

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object dfunion {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Seamless").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    //  val us_data = sc.textFile("file:///C:/Users/abhis/Downloads/sourcefiles/usdata.csv")

    val struct_schema = StructType(StructField("txnno", StringType, true) ::
      StructField("txndate", StringType, true) ::
      StructField("custno", StringType, true) ::
      StructField("amount", StringType, true) ::
      StructField("category", StringType, true) ::
      StructField("product", StringType, true) ::
      StructField("state", StringType, true) ::
      StructField("city", StringType, true) ::
      StructField("spendby", StringType, true) ::
      StructField("firstname", StringType, true) ::
      StructField("lastname1", StringType, true) ::
      StructField("company1", StringType, true) ::
      StructField("address1", StringType, true) ::
      StructField("city1", StringType, true) ::
      StructField("zip1", StringType, true) ::
      StructField("age1", StringType, true) ::
      StructField("phone1", StringType, true) ::
      StructField("phone2", StringType, true) ::
      StructField("email1", StringType, true) ::
      StructField("web", StringType, true) :: Nil)

    val df1 = spark.read.format("csv").schema(struct_schema).option("inferSchema", "true").load("file:///C:/Users/abhis/Downloads/sourcefiles/txns.txt")

    val df2 = spark.read.format("csv").schema(struct_schema).option("header", "true").option("inferSchema", "true").load("file:///C:/Users/abhis/Downloads/sourcefiles/usdata.csv")

    val df3 = df1.union(df2)
    
    df3.show() 

  }
}