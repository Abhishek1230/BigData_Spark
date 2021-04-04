package joins

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{ col, expr }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object joins {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DSL").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = new SparkSession.Builder().config(conf).getOrCreate()
    import spark.implicits._

    val txns_df = spark.read.format("csv").option("header", "true").load("file:///C:/Users/abhis/Downloads/sourcefiles/txns").limit(500)
    val us_df = spark.read.format("csv").option("header", "true").load("file:///C:/Users/abhis/Downloads/sourcefiles/usdata.csv").limit(500)

    val df1 = txns_df.withColumn("id", monotonically_increasing_id())
    val df2 = us_df.withColumn("id", monotonically_increasing_id())
    
//  val w = Window.orderBy("first_name")
//  val df1 = txns_df.withColumn("index", row_number().over(w)).show()
//  val df2 = us_df.withColumn("index", row_number().over(w)).show()
    
    val join_df = df1.join(df2,"inner").drop(df1("id"))
    join_df.show()

  }
}