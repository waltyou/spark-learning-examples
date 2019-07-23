package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by waltyou on 18-6-14.
  */
object WordCount {

  def main(args: Array[String]) {
    //Create conf object
    val conf = new SparkConf()
      .setAppName("WordCount")

    //create spark context object
    val sc = new SparkContext(conf)
    //Check whether sufficient params are supplied
    if (args.length < 2) {
      println("Usage: ScalaWordCount <input> <output>")
      System.exit(1)
    }

    //Read file and create RDD
    val rawData = sc.textFile(args(0))

    //convert the lines into words using flatMap operation
    val words = rawData.flatMap(line => line.split(" "))

    //count the individual words using map and reduceByKey operation
    val wordCount = words.map(word => (word, 1)).reduceByKey(_ + _)

    //Save the result
    wordCount.saveAsTextFile(args(1))
    //stop the spark context
    sc.stop

  }

}
