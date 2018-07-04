package cc.xmccc.sparkdemo

import cc.xmccc.hbase.util.HBaseUtil
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.spark.sql.SparkSession
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import org.pcap4j.packet.{Packet, UdpPacket}
import org.pcap4j.packet.factory.PacketFactories
import org.pcap4j.packet.namednumber.{DataLinkType, IpNumber}

import scala.util.Try

object ExtractUDPSession {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.master", "hmaster.hbase:60000")
  val conn = ConnectionFactory.createConnection(conf)
  val admin = conn.getAdmin()

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("ExtractUDPSession")
      .getOrCreate()

    val input_table = args(0)
    val save_table = args(1)

    val input_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte])](input_table)
      .select("di", "si", "dp", "sp", "pr", "t", "r", "m" )
      .inColumnFamily("p")

    if (! admin.tableExists(TableName.valueOf(save_table)))
      createPresplitTable(save_table)

    val udp_format_rdd = input_rdd map {
      case (rowkey, dip, sip, dport, sport, proto, ts_b, rawpacket, mark_b) =>
        val mark_n = BigInt(mark_b)
        val ts = BigInt(Array(0.toByte) ++ ts_b)

        if (IpNumber.getInstance(proto(0)) == IpNumber.UDP) {
          Some(mark_n, mark_b, ts, rowkey)
        } else {
          None
        }
    } filter {
      case None => false
      case _ => true
    } map {_.get}

    println("UDP packets count: ")
    println(udp_format_rdd.count())

    val sessions_list_rdd = udp_format_rdd groupBy(_._1) map {
      case (_, it) =>
        val items = it.toList.sortBy(_._3)
        val ts_seq = items map {_._3}
        val session_range =
          ts_seq zip (ts_seq drop 1) map {x => (x._1, x._2 - x._1)} filter {_._2 > 60} map {_._1}

        items.foldLeft((List.empty[(Array[Byte], Array[Byte])], ts_seq(0) :: session_range)){
          case((result, tss), item) =>
            val ts = item._3
            val mark_b = item._2
            val rowkey = item._4

            tss match {
              case first :: second :: _  if (ts >= first && ts <= second) =>
                ((rowkey, mark_b ++ ensureXByte(first.toByteArray, 8)) :: result, tss)
              case first :: second :: _ =>
                ((rowkey, mark_b ++ ensureXByte(second.toByteArray, 8)) :: result, tss.drop(1))
              case first :: Nil =>
                ((rowkey, mark_b ++ ensureXByte(first.toByteArray, 8)) :: result, tss)
            }
        }
    }

    val saved_rdd = sessions_list_rdd flatMap {x => x._1}

    println("UDP tuple5 count: ")
    println(sessions_list_rdd.count())

    println("UDP sessions count: ")
    println(saved_rdd.groupBy(x => BigInt(x._2)).count())

    println("UDP packets with session id count: ")
    println(saved_rdd.count())

    saved_rdd
      .toHBaseTable(save_table)
      .toColumns("sid")
      .inColumnFamily("p")
      .save()

    sparkSession.close()
  }

  def ensureXByte(v: Array[Byte], x: Int): Array[Byte] = {
    val len = v.length
    if(len < x) {
      Array.fill(x - len)(0.toByte) ++ v
    } else {
      v.drop(1)
    }
  }

  def parsePacket(rawpacket: Array[Byte]): Packet = {
    PacketFactories.getFactory(classOf[Packet], classOf[DataLinkType])
      .newInstance(rawpacket, 0, rawpacket.length, DataLinkType.EN10MB)
  }

  def createPresplitTable(name: String): Unit = {
    val table_name_create_presplit = TableName.valueOf(name)
    val table_presplit = new HTableDescriptor(table_name_create_presplit)
    table_presplit.addFamily(new HColumnDescriptor("p"))
    //config table to presplit rowkey
    table_presplit.setValue(HTableDescriptor.SPLIT_POLICY, classOf[KeyPrefixRegionSplitPolicy].getName)
    //set the prefix length of rowkey for presplit
    table_presplit.setValue(KeyPrefixRegionSplitPolicy.PREFIX_LENGTH_KEY, "16")

    admin.createTable(
      table_presplit,
      HBaseUtil.getHexSplits(
        Array.fill(16)(0.toByte),
        Array.fill(16)(254.toByte),
        15
      )
    )
  }
}
