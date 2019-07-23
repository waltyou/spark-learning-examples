package org.apache.spark.practices

import com.rockymadden.stringmetric.similarity.DiceSorensenMetric
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.BoundedPriorityQueue

/**
  * There have a input file A and a big file B. There are all query string in files.
  *
  * Find the Top N that is most similar to each query statement in the A file and the query statement in the B file.
  *
  */
object TopNExample {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("top N")
    val sc = new SparkContext(conf)

    // query1 is input query
    // query2 is big file
    val query1 = sc.textFile(args(0))
    val query2 = sc.textFile(args(1))
    val n = args(2).toInt

    val cartesian = query1 cartesian query2

    val result = cartesian
      .map(p => (p._1, getPriorityQueue(p._1, p._2, n)))
      .reduceByKey(_ ++= _)
      .map(p => toStringPair(p))

    println(result.collect().mkString("\n"))

  }

  def getPriorityQueue(query1: String, query2: String, n: Int): BoundedPriorityQueue[(String, Double)] = {
    val q = new BoundedPriorityQueue[(String, Double)](n)(Ordering.by(t => t._2))
    val value = DiceSorensenMetric(1).compare(query1, query2).get
    q += ((query1, value))
    q
  }

  def toStringPair(p: (String, BoundedPriorityQueue[(String, Double)])): String = {
    p._1 + " " + p._2.mkString(" ")
  }

}
