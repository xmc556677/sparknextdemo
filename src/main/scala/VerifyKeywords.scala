package cc.xmccc.sparkdemo

import java.security.MessageDigest
import java.util.Base64

import cc.xmccc.sparkdemo.Utils.repr_string
import cc.xmccc.sparkdemo.schema.ProtoModelTable
import cc.xmccc.sparkdemo.schema.HBaseOpsUtil._
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.SparkSession

import scala.util.Try



object VerifyKeywords {
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

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("ExtractFuzzySetKeywords")
      .getOrCreate()

    val input_table = args(0)
    val input2_table = args(1)

    val models = sparkSession.sparkContext.hbaseTable[(Array[Byte], Option[Array[Byte]], Option[Array[String]], Option[Array[(Double, Double)]], Option[String], Option[String])](input_table)
      .select("id", "features_name", "features_value", "fkeywords", "bkeywords")
      .inColumnFamily("model")
      .map(item => ProtoModelTable(item._1, item._2, item._3, item._4, item._5, item._6))
      .collect()

    val input_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Option[Array[Byte]], Option[String], Option[Array[Byte]], Array[Byte])](input2_table)
      .select("sid", "direction", "payload", "t")
      .inColumnFamily("p")

    val sessn_pkts_rdd = input_rdd.filter{
      case(_, Some(_), Some(_), _, _) => true
      case _ => false
    }

    val plds_direction_rdd = sessn_pkts_rdd.groupBy{
      case(_, Some(sid), Some(direction), _, t) =>
        (BigInt(sid), direction)
    }.map{
      case((_, direction), rows) =>
        val row_list = rows.toList
        val plds = row_list.sortBy(r => BigInt(r._5)).reverse.map(x => Bytes.toString(x._4.get)).mkString("")
        (direction, plds)
    }

    val forward_plds_rdd = plds_direction_rdd.filter(_._1 == "forward").map(_._2)
    val back_plds_rdd = plds_direction_rdd.filter(_._1 == "back").map(_._2)

    val verify_result = models.map{
      row =>
        val fkeywords = row.fkeywords.get.split(",").map(x => Bytes.toString(Base64.getDecoder.decode(x))).toList.filter(_.length >= 1)
        val bkeywords = row.bkeywords.get.split(",").map(x => Bytes.toString(Base64.getDecoder.decode(x))).toList.filter(_.length >= 1)
        val fresult = forward_plds_rdd.map{
          pld =>
            fkeywords.map(kw => pld.contains(kw)).foldLeft(true){case(a, b) => a && b} && fkeywords.length != 0
        }.collect().toList
        val bresult = back_plds_rdd.map{
          pld =>
            bkeywords.map(kw => pld.contains(kw)).foldLeft(true){case(a, b) => a && b} && bkeywords.length != 0
        }.collect().toList
        (Bytes.toString(row.id.get), (Try{fresult.filter(x => x).length / fresult.length.toDouble}.toOption,
          Try{bresult.filter(x => x).length / bresult.length.toDouble}.toOption))
    }

    println(s"proto\tforward_accuracy\tback_accuracy")
    verify_result.foreach{
      case(proto, (f_accuracy, b_accuracy)) =>
        println(s"$proto\t$f_accuracy\t$b_accuracy ")
    }

    sparkSession.close()
  }
}
