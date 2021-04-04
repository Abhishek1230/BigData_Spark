package Spark_DSL

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{ col, expr, lit }

object Spark_DSL {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DSL").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = new SparkSession.Builder().config(conf).getOrCreate()
    import spark.implicits._

    val struct = StructType(StructField("txnno", StringType, true) ::
      StructField("txndate", StringType, true) ::
      StructField("custno", StringType, true) ::
      StructField("amount", StringType, true) ::
      StructField("category", StringType, true) ::
      StructField("product", StringType, true) ::
      StructField("state", StringType, true) ::
      StructField("city", StringType, true) ::
      StructField("spendby", StringType, true) :: Nil)

    val txns_read = spark.read.schema(struct).format("csv").load("file:///C://Users//abhis//Downloads//sourcefiles//txns")

    println("**********************DSL Result**************************")

    //  val df_dsl = txns_read.select("txnno","txndate","custno","amount","category","spendby")
    //                            .filter(col("category") === "Gymnastics" && col("spendby") ==="credit")

    //  val df_dsl = txns_read.select("txnno","txndate","custno","amount","category","spendby")
    //                            .filter(col("category") like "%Sports%")

    //  val df_dsl = txns_read.select("txnno","txndate","custno","amount","category","spendby")
    //                            .filter(col("category").isin("Team Sports", "Water Sports"))
    //  val df_dsl = txns_read.select("txnno","txndate","custno","amount","category","spendby")
    //                            .filter(col("category").isNotNull)
    //    val df_dsl = txns_read.selectExpr("txnno","txndate","substring(txndate,7,4)as year","custno","amount","category","spendby")
    //                            .filter(col("category").isNotNull)

    //    val df_dsl = txns_read.selectExpr("txnno","txndate","substring(txndate,7,4)as year","custno","amount","trim(category) as category","spendby",
    //                                       "case when category='Exercise & Fitness' then 1 else 0 end as code")
    //                                      .filter(col("category").isNotNull && col("spendby").isNotNull)

    println("*********************selectexpr result*****************************")
    val df_dsl = txns_read.selectExpr("txnno", "txndate", "custno", "amount", "split(category,' ')[0] as category", "product", "state", "city", "spendby")

    df_dsl.show()

    println("*********************withColumn result*****************************")
    val ds_dsl1 = txns_read.withColumn("category", expr("split(category,' ')[0]"))
                            .withColumn("product", expr("split(product,' ')[1]"))                            
                             .withColumn("txndate", expr("from_unixtime(unix_timestamp(txndate,'MM-dd-yyyy'))"))
                             .withColumn("token", lit(5)).show()
                             
    

  }

}