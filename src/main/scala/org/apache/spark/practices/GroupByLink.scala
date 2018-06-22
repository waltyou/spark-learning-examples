package org.apache.spark.practices

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.SortedSet
import scala.collection.mutable.{HashSet, ListBuffer}

object GroupByLink {

  private val FIRST_SPLIT_CHAR = ","

  private val SECOND_SPLIT_CHAR = ":"

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("ConnectedComponents mapreduce")
    val sc = new SparkContext(conf)
    // read file
    val baseInfo = sc.textFile("data/graphx/peoples.txt")

    var userLinks = baseInfo.flatMap(line => map1(line)).reduceByKey(_ + FIRST_SPLIT_CHAR + _).values

    userLinks = sortUniq(userLinks)

    var result = linkUser(userLinks)
    result = linkUser(result)

    println(result.collect().mkString("\n"))

  }

  def map1(line: String): List[(String, String)] = {
    val array = line.split(FIRST_SPLIT_CHAR)
    val id = array(0)
    val resultSet: HashSet[(String, String)] = HashSet()
    if (array.length > 2) {
      val fields = array(2).split(SECOND_SPLIT_CHAR)
      for (field <- fields) {
        resultSet.add((field, id))
      }
    } else {
      resultSet.add((id, id))
    }
    resultSet.toList
  }

  def sortUniq(input: RDD[String]): RDD[String] = {
    input.map(l => (l, 1)).reduceByKey(_ + _).keys
  }

  def linkUser(input: RDD[String]): RDD[String] = {
    val tmp = input.flatMap(line => map2(line)).reduceByKey((a, b) => reduce1(a, b)).values
    sortUniq(tmp)
  }

  def map2(line: String): List[(String, String)] = {
    var allList = new ListBuffer[(String, String)]()
    if (line.contains(FIRST_SPLIT_CHAR)) {
      val fieldsSet = line.split(FIRST_SPLIT_CHAR).toSet[String]
      fieldsSet.foreach(x => {
        allList.+=((x, line))
      })
    }
    else {
      allList.+=((line, line))
    }
    allList.toList
  }

  def reduce1(a: String, b: String): String = {
    val aSet = a.split(FIRST_SPLIT_CHAR).to[SortedSet]
    val bSet = b.split(FIRST_SPLIT_CHAR).to[SortedSet]
    val cSet = aSet ++ bSet
    cSet.toString().replace("TreeSet(", "").replace(")", "").replace(" ", "")
  }
}
