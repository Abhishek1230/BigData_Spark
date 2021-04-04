package Sparkusecase

import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object Sparkusecase {

  case class Schema(Direction: String, Year: String, Date: String, Weekday: String, Current_Match: String, Country: String, Commodity: String,
                    Transport_Mode: String, Measure: String, Value: String, Cumulative: String)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("usecase").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    sc.setLogLevel("ERROR")

    val spark = new SparkSession.Builder().config(conf).getOrCreate()
    import spark.implicits._

    val read_rdd = sc.textFile("file:///C:/Users/abhis/Downloads/sourcefiles/coviddata/covid-data.txt")
    val header = read_rdd.first()
    val rem_header = read_rdd.filter(x => !x.contains(header))
//  rem_header.foreach(println)

    val schema_impose = rem_header.map(x => x.split(",")).map(x => Schema(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10)))

    val filter_schema = schema_impose.filter(x => x.Measure == "Tonnes")
    
    filter_schema.foreach(println)
    
    filter_schema.count()
    
    val df1 = filter_schema.toDF()
    
    df1.createOrReplaceTempView("dataframe1")

    val row_rdd = rem_header.map(x => x.split(",")).map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10)))

    val filter_rowrdd = row_rdd.filter(x => x(8)=='$')
    
    val schema_file = sc.textFile("file:///C:/Users/abhis/Downloads/sourcefiles/coviddata/schema_file.txt").flatMap(x => x.split(",")).collect().toList
    
    schema_file.foreach(println)
//    println(schema_file(0))
//    println(schema_file(1))
    val schema_struct = StructType(schema_file.map(fieldName => StructField(fieldName, StringType, true)))

    println(schema_struct)
    val df2 = spark.createDataFrame(filter_rowrdd, schema_struct)
    df2.createOrReplaceTempView("dataframe2")
    
//    val tonnes_exports = spark.sql("select * from dataframe1 where Direction = 'Exports'")
//    tonnes_exports.coalesce(1).write.format("csv").mode("overwrite").option("header","true").save("file:///C:/Users/abhis/Downloads/sourcefiles/outputfiles/df_csv")
//   
//    val dollar_exports = spark.sql("select * from dataframe2 where Direction = 'Exports'")
//    dollar_exports.coalesce(1).write.format("parquet").mode("overwrite").save("file:///C:/Users/abhis/Downloads/sourcefiles/outputfiles/df2_parquet")
//       
//    val dollar_imports = spark.sql("select * from dataframe2 where Direction = 'Imports'")
//    dollar_imports.coalesce(1).write.format("json").mode("overwrite").save("file:///C:/Users/abhis/Downloads/sourcefiles/outputfiles/df3_json")
//    
//    val dollar_reimports = spark.sql("select * from dataframe2 where Direction = 'Reimports'")
//    dollar_reimports.coalesce(1).write.format("com.databricks.spark.avro").mode("overwrite").save("file:///C:/Users/abhis/Downloads/sourcefiles/outputfiles/df4_avro")
    
    val tonnes_exports = df1.filter($"Direction"==="Exports")
    tonnes_exports.coalesce(1).write.format("csv").mode("overwrite").option("header","true").save("file:///C:/Users/abhis/Downloads/sourcefiles/outputfiles/df_csv")
    val dollar_exports = df2.filter(col("Direction")==="Exports")
    dollar_exports.coalesce(1).write.format("parquet").mode("overwrite").save("file:///C:/Users/abhis/Downloads/sourcefiles/outputfiles/df2_parquet")
    val dollar_imports = df2.filter(col("Direction")==="Imports")
    dollar_imports.coalesce(1).write.format("json").mode("overwrite").save("file:///C:/Users/abhis/Downloads/sourcefiles/outputfiles/df3_json")
    val dollar_reimports = df2.filter(col("Direction")==="Reimports")
    dollar_reimports.coalesce(1).write.format("com.databricks.spark.avro").mode("overwrite").save("file:///C:/Users/abhis/Downloads/sourcefiles/outputfiles/df4_avro")
    
    val csv_df = spark.read.format("csv").option("header","true").load("file:///C:/Users/abhis/Downloads/sourcefiles/outputfiles/df_csv")
    val parquet_df = spark.read.format("parquet").option("header","true").load("file:///C:/Users/abhis/Downloads/sourcefiles/outputfiles/df2_parquet")
    val json_df = spark.read.format("json").option("header","true").load("file:///C:/Users/abhis/Downloads/sourcefiles/outputfiles/df3_json")
    val avro_df = spark.read.format("com.databricks.spark.avro").option("header","true").load("file:///C:/Users/abhis/Downloads/sourcefiles/outputfiles/df4_avro")
    
    val union_df = csv_df.union(parquet_df).union(json_df).union(avro_df)
    union_df.show(10)
    
  }

}