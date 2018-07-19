package org.apache.spark.practices

import com.rockymadden.stringmetric.similarity.DiceSorensenMetric
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.PriorityQueue

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

    val result = cartesian.map(p => (p._1, getPriorityQueue(p._1, p._2))).reduceByKey(
      (q1, q2) => mergeTwoQueue(q1, q2, n)
    ).map(p => toStringPair(p))

    println(result.collect().mkString("\n"))

  }

  def getPriorityQueue(query1: String, query2: String): PriorityQueue[(String, Double)] = {
    val q = new PriorityQueue[(String, Double)]()(Ordering.by(t => 0 - t._2))
    val value = DiceSorensenMetric(1).compare(query1, query2).get
    q.enqueue((query1, value))
    q
  }

  def mergeTwoQueue(q1: PriorityQueue[(String, Double)], q2: PriorityQueue[(String, Double)], n: Int): PriorityQueue[(String, Double)] = {
    q1.foreach(item => q2.enqueue(item))
    while (q2.size > n) {
      q2.dequeue()
    }
    q2
  }


  def toStringPair(p: (String, mutable.PriorityQueue[(String, Double)])): String = {
    p._1 + " " + p._2.mkString(" ")
  }

}
