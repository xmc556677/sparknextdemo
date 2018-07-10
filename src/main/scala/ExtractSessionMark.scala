package cc.xmccc.sparkdemo

import java.net.InetAddress

import org.apache.spark.sql.SparkSession
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.util.Bytes
import org.pcap4j.packet.namednumber.IpNumber

object ExtractSessionMark {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("ExtractSessionMark")
      .getOrCreate()

    val input_table = args(0)
    val save_table = args(1)

    val input_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Array[Byte])](input_table)
      .select("m" )
      .inColumnFamily("sessn")

    input_rdd
      .groupBy(x => BigInt(x._2))
      .map(_._2.toList)
      .collect
      .sortBy(_.length)(Ordering[Int].reverse)
      .foreach{
        list =>
          val dip_dport_proto = list(0)._2
          val dip = dip_dport_proto.slice(0, 4)
          val dport = dip_dport_proto.slice(4, 6)
          val proto = dip_dport_proto.slice(6, 7)
          val ip = InetAddress.getByAddress(dip)
          val port = BigInt(Array(0.toByte) ++ dport)

          println(s"${ip}:${port}-${IpNumber.getInstance(proto(0)).valueAsString()}\t${list.length}")
      }

    sparkSession.close()
  }
}
