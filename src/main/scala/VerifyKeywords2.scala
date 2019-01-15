package cc.xmccc.sparkdemo

import java.util.Base64

import cc.xmccc.sparkdemo.schema.HBaseOpsUtil._
import cc.xmccc.sparkdemo.schema.ProtoModelTable
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession

import scala.util.Try


object VerifyKeywords2 {
  def slicePacket(str: Array[Byte], n: Int): List[Array[Byte]] =
    (0 to str.length - n).map(i => str.slice(i, i+n)).toList

  def stringToByteArray(str: String): Array[Byte] = {
    str.toCharArray.map(_.toByte)
  }

  def stringHexToByteArray(str: String): Array[Byte] = {
    (0 to str.length-1 by 2).map{
      i =>
        val hex = str.slice(i, i+2)
        Integer.parseInt(hex, 16).toByte
    }.toArray
  }

  def stringHexToString(str: String): String = {
    (0 to str.length-1 by 2).map{
      i =>
        val hex = str.slice(i, i+2)
        Integer.parseInt(hex, 16).toChar
    }.mkString("")
  }

  def convert_binary_tostring(bin: Array[Byte]): String = {
    bin.map(x => "%02x".format(x & 0x00ff)).mkString("")
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("ExtractFuzzySetKeywords")
      .getOrCreate()

    val input_table = args(0)
    val input2_table = args(1)
    val fzset_id = args(2)
    val dest_proto = args(3)
    val fzset_id_b = (0 to fzset_id.length-1 by 2).map{
      i =>
        val hex = fzset_id.slice(i, i+2)
        Integer.parseInt(hex, 16).toByte
    }.toArray

    val models = sparkSession.sparkContext.hbaseTable[(Array[Byte], Option[Array[Byte]], Option[Array[String]], Option[Array[(Double, Double)]], Option[String], Option[String])](input_table)
      .select("id", "features_name", "features_value", "fkeywords", "bkeywords")
      .inColumnFamily("model")
      .map(item => ProtoModelTable(item._1, item._2, item._3, item._4, item._5, item._6))
      .collect()

    val input_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Option[Array[Byte]], Option[Array[Byte]], Option[String], Option[Array[Byte]], Array[Byte])](input2_table)
      .select("m", "sid", "direction", "payload", "t")
      .inColumnFamily("p")

    val sessn_pkts_rdd = input_rdd.filter{
      case(_, Some(m), Some(_), Some(_), _, _) if (BigInt(m) == BigInt(fzset_id_b))=> true
      case _ => false
    }.map(row => (row._1, row._3, row._4, row._5, row._6))

    val plds_direction_rdd = sessn_pkts_rdd.groupBy{
      case(_, Some(sid), Some(direction), _, t) =>
        (BigInt(sid), direction)
    }.map{
      case((_, direction), rows) =>
        val row_list = rows.toList
        val plds = row_list.sortBy(r => BigInt(r._5)).reverse.map(x => x._4.get).flatten.toArray
        (direction, plds)
    }

    val forward_plds_rdd = plds_direction_rdd.filter(_._1 == "forward").map(_._2)
    val back_plds_rdd = plds_direction_rdd.filter(_._1 == "back").map(_._2)

    val forward_result = forward_plds_rdd.map {
      pld =>
        val result = models.map {
          row =>
            val fkeywords = row.fkeywords.get.split(",").map(x => Base64.getDecoder.decode(x)).toList.filter(_.length >= 1)

            val match_result = fkeywords.map {
              kw =>
                pld.containsSlice(kw)
            }

            val accurancy = match_result.filter(x => x).length / match_result.length.toDouble
            (Bytes.toString(row.rowkey), accurancy)
        }.maxBy(_._2)

      result._1
    }.filter(x => x == dest_proto)
      //}.groupBy(x => x).map(item => (item._1, item._2.toList.length)).collect().toList

    val back_result = back_plds_rdd.map {
      pld =>
        val result = models.map {
          row =>
            val bkeywords = row.bkeywords.get.split(",").map(x => Base64.getDecoder.decode(x)).toList.filter(_.length >= 1)

            val match_result = bkeywords.map {
              kw =>
                pld.containsSlice(kw)
            }

            val accurancy = match_result.filter(x => x).length / match_result.length.toDouble
            (Bytes.toString(row.rowkey), accurancy)
        }.maxBy(_._2)

      result._1
    }.filter(x => x == dest_proto)
    //}.groupBy(x => x).map(item => (item._1, item._2.toList.length)).collect().toList

    //println(s"forward: ${forward_result}")
    //println(s"back: ${back_result}")
    val forward_accurancy = forward_result.count() / forward_plds_rdd.count().toDouble
    val back_accurancy = back_result.count() / back_plds_rdd.count().toDouble

    println(s"${dest_proto}\t${forward_accurancy}\t${back_accurancy}")

    sparkSession.close()
  }
}
