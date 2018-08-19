package cc.xmccc.sparkdemo

import java.security.MessageDigest
import cc.xmccc.sparkdemo.Utils.repr_string

import org.apache.spark.sql.SparkSession
import it.nerdammer.spark.hbase._

object ExtractFuzzySetMD5ADport {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("ExtractSessionFeature")
      .getOrCreate()

    val input_table = args(0)
    val save_table = args(1)

    val input_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Option[Array[Byte]], Array[Byte])](input_table)
      .select("m", "dport")
      .inColumnFamily("sessn")

    val r = input_rdd.filter(_ != None).take(1).map{
      case(_, m, dport) =>
        val dp = BigInt(dport)
        val mark = MessageDigest.getInstance("MD5").digest(m.get)

        (
          dp.toString,
          repr_string(String.valueOf(mark.map(x => Character.toChars(x & 0x00ff)).flatten))
        )
    }

    println(r(0))
  }
}
