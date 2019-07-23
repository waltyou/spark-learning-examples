package org.apache.spark.practices.graphx

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet
import scala.collection.mutable.ListBuffer

/**
  * one people have some field which people can link by them.
  *
  * This Class will find the different groups from many peoples.
  *
  * Created by waltyou on 18-6-15.
  */
object CCTest {

  private val FIRST_SPLIT_CHAR = ","

  private val SECOND_SPLIT_CHAR = ":"

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("ConnectedComponents")
    val sc = new SparkContext(conf)
    // read file
    val baseInfo = sc.textFile("data/graphx/peoples.txt")
    // graphx.connectedComponents only take Long parameter
    val baseInfoId = baseInfo.map(line => addId(line))
    // build graph
    val vertexRdd = generateVertexs(baseInfoId)
    val edgeRdd = generateEdges(baseInfoId)
    val graph = Graph(vertexRdd, edgeRdd)
    // connectedComponents
    val cc = graph.connectedComponents().vertices
    // group by the root vertex
    val ccWithUser = vertexRdd.join(cc).map {
      case (id, (user, cc)) => (user, cc)
    }.map(l => l.swap).reduceByKey(_ + FIRST_SPLIT_CHAR + _).values

    println(ccWithUser.collect().mkString("\n"))
  }

  def addId(line: String): (Long, String) = {
    val id = str2Long(line.split(FIRST_SPLIT_CHAR)(0))
    (id, line)
  }

  def generateVertexs(baseInfoId: RDD[(Long, String)]): RDD[(Long, String)] = {
    baseInfoId.map(p => (p._1, p._2.split(FIRST_SPLIT_CHAR)(0)))
  }

  def generateEdges(baseInfoId: RDD[(Long, String)]): RDD[Edge[String]] = {
    val userIdPairs = baseInfoId.flatMap(p => returnUserFieldPairs(p._1, p._2))
      .map(l => firstMap(l)).reduceByKey(_ + FIRST_SPLIT_CHAR + _)
    userIdPairs.flatMap(p => generateEdgeList(p._2))
  }

  def returnUserFieldPairs(id: Long, line: String): List[String] = {
    val array = line.split(FIRST_SPLIT_CHAR)
    var resultSet: Set[String] = HashSet()
    if (array.length > 2) {
      val fields = array(2).split(SECOND_SPLIT_CHAR)
      for (field <- fields) {
        resultSet += id.toString + FIRST_SPLIT_CHAR + field
      }
    } else {
      resultSet += id.toString
    }
    resultSet.toList
  }

  def str2Long(s: String): Long = s.##.toLong

  def firstMap(l: String): (String, String) = {
    val field = l.split(FIRST_SPLIT_CHAR)
    if (field.length > 1) {
      (field(1), field(0))
    } else {
      (field(0), field(0))
    }
  }

  def generateEdgeList(line: String): List[Edge[String]] = {
    var allSet = new ListBuffer[Edge[String]]()
    if (line.contains(FIRST_SPLIT_CHAR)) {
      val field = line.split(FIRST_SPLIT_CHAR).toSet[String].toArray[String]
      for (i <- 0 until field.length - 1) {
        for (j <- i + 1 until field.length) {
          allSet += Edge(field(i).toLong, field(j).toLong)
        }
      }
    }
    else {
      allSet += Edge(line.toLong, line.toLong)
    }
    allSet.toList
  }
}
