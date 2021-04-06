package Complexdata

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Complexdata {
  
    def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DSL").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = new SparkSession.Builder().config(conf).getOrCreate()
    import spark.implicits._
  
    val emp_df1 = spark.read.option("multiline","true").format("json").load("file:///C:/Users/abhis/Downloads/sourcefiles/emp_json/emp_data.json")
    emp_df1.printSchema()
    emp_df1.show(false)
    emp_df1.select("Employee_id","Employee_name","Employee_client").show()
    
    val emp_df2 = spark.read.option("multiline","true").format("json").load("file:///C:/Users/abhis/Downloads/sourcefiles/emp_json/emp_data1.json")
    emp_df2.printSchema()
    emp_df2.show(false)
    emp_df2.select("Employee.id","Employee.name","Employee.client").show()
    
    val emp_df3 = spark.read.option("multiline","true").format("json").load("file:///C:/Users/abhis/Downloads/sourcefiles/emp_json/emp_data2.json")
    emp_df3.printSchema()
    emp_df3.show(false)    
    emp_df3.select("Employee1.id","Employee2.id","Nationality","Native","Place").show()
    
    val emp_df4 = spark.read.option("multiline","true").format("json").load("file:///C:/Users/abhis/Downloads/sourcefiles/emp_json/emp_data3.json")
    emp_df4.printSchema()
    emp_df4.show()
    emp_df4.withColumn("Employees",explode(col("Employees")))
        .select("Employees.*","Nationality","Native","Place").show()
    
    }
    
}