package com.spark.spark_testing

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import com.spark.spark_testing._

object Book {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.sql.warehouse.dir", "E://temp")
      .config("spark.master", "local[2]")
      .getOrCreate()

      
    val bookDf = spark.sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "book")
      .schema(new CustomSchema().customSchema)
      .load("src/main/resources/book.xml")
    
    
    bookDf.printSchema()
    bookDf.show()
  }
}