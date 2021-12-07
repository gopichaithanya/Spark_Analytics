package org.spark



import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object spark_test02 extends App {
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  val spark = SparkSession.builder().master("local[3]").getOrCreate()
  import spark.implicits._
  val dataFile = spark.sparkContext.textFile("data//word.txt")
  val resultDF = dataFile.flatMap(line => line.split(" ")).map(word =>(word,1)).reduceByKey(_+_).toDF("words","count")
  resultDF.show()
}
