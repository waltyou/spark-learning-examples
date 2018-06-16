package org.apache.spark.examples.graphx

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by waltyou on 18-6-15.
  */
object ConnectedComponents {

  def main(args: Array[String]) {

    //Create conf object
    val conf = new SparkConf()
      .setAppName("ConnectedComponents")

    //create spark context object
    val sc = new SparkContext(conf)
    // Load the graph as in the PageRank example
    val graph = GraphLoader.edgeListFile(sc, "data/graphx/followers.txt")
    // Find the connected components
    val cc = graph.connectedComponents().vertices
    // Join the connected components with the usernames
    val users = sc.textFile("data/graphx/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    // Print the result
    println(ccByUsername.collect().mkString("\n"))
  }
}
