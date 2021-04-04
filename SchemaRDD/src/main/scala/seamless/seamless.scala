package seamless

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object seamless {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Seamless").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val txn_data = sc.textFile("file:///C:/Users/abhis/Downloads/sourcefiles/txns.txt")

    //seamless
    val df1 = spark.read.format("csv").load("file:///C:/Users/abhis/Downloads/sourcefiles/txns.txt")

    df1.show()

    df1.printSchema()

    df1.createOrReplaceTempView("txns_seamless")

/*    val df2 = spark.sql("select _c0 from txns_seamless")

    df2.show()*/

    val struct_schema = StructType(StructField("txnno", StringType, true) ::
      StructField("txndate", StringType, true) ::
      StructField("custno", StringType, true) ::
      StructField("amount", StringType, true) ::
      StructField("category", StringType, true) ::
      StructField("product", StringType, true) ::
      StructField("state", StringType, true) ::
      StructField("city", StringType, true) ::
      StructField("spendby", StringType, true) :: Nil)

    val df3 = spark.read.format("csv").schema(struct_schema).option("inferschema", "true").load("file:///C:/Users/abhis/Downloads/sourcefiles/txns")

    df3.show()
    
    df3.printSchema()

  }

}