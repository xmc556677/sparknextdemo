package cc.xmccc.sparkdemo

import cc.xmccc.hbase.util.HBaseUtil
import org.apache.spark.sql.SparkSession
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import org.apache.hadoop.hbase.util.Bytes
import org.pcap4j.packet._
import org.pcap4j.packet.factory.PacketFactories
import org.pcap4j.packet.namednumber.{DataLinkType, IpNumber}

import scala.util.Try

object ExtractTuple5 {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.master", "hmaster.hbase:60000")
  val conn = ConnectionFactory.createConnection(conf)
  val admin = conn.getAdmin()

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("ExtractTuple5")
      .getOrCreate()

    val input_table = args(0)
    val save_table = args(1)
    val input_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Array[Byte], Array[Byte])](input_table)
      .select("r", "t" )
      .inColumnFamily("p")

    if (! admin.tableExists(TableName.valueOf(save_table)))
      createPresplitTable(save_table)

    val tuple5_rdd = input_rdd.map{
      case (rowkey, raw_packet, ts_byte) =>
        val tcp_tuple5 = Try {
          val eth = parsePacket(raw_packet)
          val ipv4 = eth.get(classOf[IpV4Packet])
          val ipv4h = ipv4.getHeader

          val dip = ipv4h.getDstAddr.getAddress
          val sip = ipv4h.getSrcAddr.getAddress
          val proto = Array.fill(1)(ipv4h.getProtocol.value.toByte)

          val tcp = ipv4.get(classOf[TcpPacket])
          val tcph = tcp.getHeader
          val dport = Bytes.toBytes(tcph.getDstPort.value)
          val sport = Bytes.toBytes(tcph.getSrcPort.value)

          (rowkey, dip, sip, dport, sport, proto, raw_packet, ts_byte)
        } toOption

        val udp_tuple5 = Try {
          val eth = parsePacket(raw_packet)
          val ipv4 = eth.get(classOf[IpV4Packet])
          val ipv4h = ipv4.getHeader

          val dip = ipv4h.getDstAddr.getAddress
          val sip = ipv4h.getSrcAddr.getAddress
          val proto = Array.fill(1)(ipv4h.getProtocol.value.toByte)

          val udp = ipv4.get(classOf[UdpPacket])
          val udph = udp.getHeader
          val dport = Bytes.toBytes(udph.getDstPort.value)
          val sport = Bytes.toBytes(udph.getSrcPort.value)

          (rowkey, dip, sip, dport, sport, proto, raw_packet, ts_byte)
        } toOption

        val icmp_tuple5 = Try {
          val eth = parsePacket(raw_packet)
          val ipv4 = eth.get(classOf[IpV4Packet])
          val ipv4h = ipv4.getHeader

          val dip = ipv4h.getDstAddr.getAddress
          val sip = ipv4h.getSrcAddr.getAddress
          val proto = Array.fill(1)(ipv4h.getProtocol.value.toByte)

          if(IpNumber.getInstance(proto(0)) == IpNumber.ICMPV4) {
            (rowkey, dip, sip, Bytes.toBytes(0.toShort), Bytes.toBytes(0.toShort), proto, raw_packet, ts_byte)
          } else {
            throw new Exception()
          }
        }

        (tcp_tuple5, udp_tuple5) match {
          case (Some(v), _) => Some(v)
          case (_, Some(v)) => Some(v)
          case _ => None
        }
    }

    val tuple5_filter_rdd = tuple5_rdd.filter {
      case None => false
      case _ => true
    }.map(a => a.get)

    tuple5_filter_rdd
      .toHBaseTable(save_table)
      .toColumns("di", "si", "dp", "sp", "pr", "r", "t")
      .inColumnFamily("p")
      .save()

    sparkSession.close()
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
