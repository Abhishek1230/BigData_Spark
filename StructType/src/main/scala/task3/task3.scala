package task3

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object task3 {

  case class schema(txnno: Int, txnDate: String, custno: String, amount: String, category: String, product: String, city: String, state: String, transtype: String)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("txnsunion").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = new SparkSession.Builder().config(conf).getOrCreate()
    import spark.implicits._

    val txns_rdd = sc.textFile("file:///C:/Users/abhis/Downloads/sourcefiles/txns.txt")
    
    val filter_data = txns_rdd.map(x => x.split(",")).map(x => schema(x(0).toInt, x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))

    val df1 = filter_data.toDF()
    
//  df1.show()

    val row_rdd = txns_rdd.map(x => x.split(",")).map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))

    val struct_schema = StructType(StructField("txnno", StringType, true) ::
      StructField("txndate", StringType, true) ::
      StructField("custno", StringType, true) ::
      StructField("amount", StringType, true) ::
      StructField("category", StringType, true) ::
      StructField("product", StringType, true) ::
      StructField("state", StringType, true) ::
      StructField("city", StringType, true) ::
      StructField("spendby", StringType, true) :: Nil)

    val df2 = spark.createDataFrame(row_rdd, struct_schema)
    
//  df2.show()
    
    val df3 = spark.read.format("csv").schema(struct_schema).option("inferschema" , "true").load("file:///C:/Users/abhis/Downloads/sourcefiles/txns.txt")
    
//  df3.show()
    
    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")
    df3.createOrReplaceTempView("df3")
    
    val union = spark.sql("select * from df1 UNION select * from df2 UNION select * from df3")
    
    union.printSchema()
    
    union.show()

  }
}