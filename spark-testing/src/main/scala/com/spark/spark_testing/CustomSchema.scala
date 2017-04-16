package com.spark.spark_testing

import org.apache.spark.sql.types._

 class CustomSchema {
  
  def customSchema = StructType(Array(
        StructField("_id",StringType,true),
        StructField("author",StringType,true),
        StructField("description",StringType,true),
        StructField("genre",StringType,true),
        StructField("price",DoubleType,true),
        StructField("publish_date",StringType,true),
        StructField("title",StringType,true)
        ))
}