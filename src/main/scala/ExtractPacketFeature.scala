package cc.xmccc.sparkdemo

import cc.xmccc.hbase.util.HBaseUtil
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.pcap4j.packet.factory.PacketFactories
import org.pcap4j.packet.namednumber.{DataLinkType, IpNumber}
import org.pcap4j.packet.{EthernetPacket, IpV4Packet, Packet, TcpPacket}

import scala.util.{Failure, Success, Try}

object ExtractPacketFeature {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.master", "hmaster.hbase:60000")
  val conn = ConnectionFactory.createConnection(conf)
  val admin = conn.getAdmin()

  def main(args: Array[String]): Unit = {
   val sparkSession = SparkSession.builder()
      .appName("ExtractSessionFeature")
      .getOrCreate()

    val input_table = args(0)
    println(s"input table: ${input_table}")
    val save_table = args(1)
    println(s"output table: ${save_table}")

    val input_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Array[Byte], Array[Byte])](input_table)
      .select("t", "r" )
      .inColumnFamily("p")

    if (! admin.tableExists(TableName.valueOf(save_table)))
      createPresplitTable(save_table)

    val save_rdd = input_rdd.map{
      case(rowkey, ts, rawpkt) =>
        val pkt = parsePacket(rawpkt)

        Try {
          val ipv4 = pkt.get(classOf[IpV4Packet])
          val ipv4h = ipv4.getHeader
          val tcp = ipv4.get(classOf[TcpPacket])
          val tcph = tcp.getHeader

          val dport_b = Bytes.toBytes(tcph.getDstPort.value)
          val sport_b = Bytes.toBytes(tcph.getSrcPort.value)
          val proto = Array.fill(1)(ipv4h.getProtocol.value.toByte)

          val flags_b = tcp.getRawData.slice(13, 14)

          val pkt_len_b = Bytes.toBytes(rawpkt.length)
          val payload_len = Try{
            tcp.getPayload.getRawData.length
          } getOrElse(0)
          val payload_len_b = Bytes.toBytes(payload_len)

          (rowkey, dport_b, sport_b, proto,flags_b, pkt_len_b, payload_len_b, ts)
        } toOption
    }.filter{
      case None => false
      case _ => true
    }.map{_.get}

    println(s"saved packets: ${save_rdd.count()}")

    save_rdd
      .toHBaseTable(save_table)
      .toColumns("dport", "sport", "proto", "tcp_flags", "pkt_len", "pld_len", "t")
      .inColumnFamily("p")
      .save()
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
