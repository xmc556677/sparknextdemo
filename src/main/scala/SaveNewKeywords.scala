package cc.xmccc.sparkdemo

import java.util.Base64

import org.apache.spark.sql.SparkSession
import it.nerdammer.spark.hbase._

import scala.io.Source


object SaveNewKeywords {

  def hexToBytesArray(str: String) = {
    val result = (0 to str.length-1 by 2).map{
      i =>
        val hex = str.slice(i, i+2)
        Integer.parseInt(hex, 16).toByte
    }.toArray

    result
  }

  def hexToString(str: String): String = {
    val result = (0 to str.length-1 by 2).map{
      i =>
        val hex = str.slice(i, i+2)
        Integer.parseInt(hex, 16).toChar
    }.mkString("")
    result
  }

  def encodeWithBase64(arr_b: Array[Byte]) = {
    Base64.getEncoder.encodeToString(arr_b)
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("SaveNewKeywords")
      .getOrCreate()

    val input_source = args(0)
    val output_table = args(1)

    val kw_table = Source
      .fromFile(input_source)
      .getLines()
      .toList
      .map{
        line =>
          val items = line
            .split(",")

          val result = items
            .tail
            .tail
            .map(encodeWithBase64 _ compose hexToBytesArray _)
            .mkString(",")
          (hexToString(items.head), hexToString(items.tail.head), result)
      }.groupBy(item => item._1)
      .map{
        case(proto, ls) =>
          println(ls)
          val forwards = ls.filter(_._2 == "forward").head._3
          val back = ls.filter(_._2 == "back").head._3

          (proto, forwards, back)
      }.toList

    val kw_rdd = sparkSession.sparkContext.parallelize(kw_table)

    kw_rdd.toHBaseTable(output_table)
      .toColumns("fkeywords", "bkeywords")
      .inColumnFamily("model")
      .save()

  }

}
