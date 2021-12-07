package org.spark

import org.apache.spark.sql.SparkSession

object spark_test01 extends App{

  val spark = SparkSession.builder().master("local[3]").appName("Spark_App").getOrCreate()
  println(spark)

}
