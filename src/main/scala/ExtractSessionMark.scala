package cc.xmccc.sparkdemo

import java.net.InetAddress

import org.apache.spark.sql.SparkSession
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.pcap4j.packet.namednumber.IpNumber

object ExtractSessionMark {

  def PktDirectionAndFuzzySetMarkExtract(tuple5_sid_rdd: RDD[(Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Option[Array[Byte]])]): RDD[(Array[Byte], String, Array[Byte])] = {
    val saved_rdd = tuple5_sid_rdd.filter(_._8 != None).groupBy(x => BigInt(x._8.get))
      .flatMap{
        case(_, it) =>
          val items = it.toList.sortBy(x => BigInt(Array(0.toByte) ++ x._7))
          val direction_seq = items.map{
            case(r, si, sp, di, dp, proto, _, _) =>
              val src = BigInt(si ++ sp)
              val dst = BigInt(di ++ dp)
              (r, (src, dst))
          }

          val (_, _, _, di, dp, proto, _, _) = items(0)
          val fuzzyset_mark = di ++ dp ++ proto

          val forward = direction_seq(0)
          direction_seq.map{
            case(r, direction) =>
              if(direction == forward._2)
                (r, "forward", fuzzyset_mark)
              else
                (r, "back", fuzzyset_mark)
          }
      }

    saved_rdd
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("ExtractSessionMark")
      .getOrCreate()

    val input_table = args(0)
    val save_table = args(1)

    val tuple5_sid_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Option[Array[Byte]])](input_table)
      .select("si", "sp", "di", "dp", "pr", "t", "sid")
      .inColumnFamily("p")

    val pkt_direction_m_rdd = PktDirectionAndFuzzySetMarkExtract(tuple5_sid_rdd)

    pkt_direction_m_rdd.toHBaseTable(save_table)
        .toColumns("direction", "m")
        .inColumnFamily("p")
        .save()

    sparkSession.close()
  }
}
