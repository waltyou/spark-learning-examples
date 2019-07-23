package org.apache.spark.examples.sql

import org.apache.spark.sql.SparkSession


object SparkSQLExample {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    runBasicDataFrameExample(spark)
    runDatasetCreationExample(spark)
    runInferSchemaExample(spark)
    runProgrammaticSchemaExample(spark)

    spark.stop()
  }

  private def runBasicDataFrameExample(spark: SparkSession): Unit = {
    import spark.implicits._

    val df = spark.read.json("data/sql/people.json")
    df.printSchema()
    df.show()
    df.select($"name", $"age" + 1).show()
    df.select($"name").show()
    df.filter($"age" > 21).show()
    df.groupBy("age").count().show()

    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()

    df.createGlobalTempView("people")
    spark.sql("SELECT * FROM global_temp.people").show()
    spark.newSession().sql("SELECT * FROM global_temp.people").show()
  }

  case class Person(name: String, age: Long)

  private def runDatasetCreationExample(spark: SparkSession): Unit = {
    import spark.implicits._

    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()
  }

  private def runInferSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._
  }

  private def runProgrammaticSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._
  }
}