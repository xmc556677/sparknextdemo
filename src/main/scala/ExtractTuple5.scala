package cc.xmccc.sparkdemo

import cc.xmccc.hbase.util.HBaseUtil
import org.apache.spark.sql.SparkSession
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import org.apache.hadoop.hbase.util.Bytes
import org.pcap4j.packet.{EthernetPacket, IpV4Packet, TcpPacket, UdpPacket}

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

    val input_table = "xmc:rawpackets_2gb"
    val save_table = "xmc:tuple5_2gb_p"
    val input_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Array[Byte], Array[Byte])](input_table)
      .select("r", "t" )
      .inColumnFamily("p")

    if (! admin.tableExists(TableName.valueOf(save_table)))
      createPresplitTable(save_table)

    val tuple5_rdd = input_rdd.map{
      case (rowkey, raw_packet, ts_byte) =>
        val tcp_tuple5 = Try {
          val eth = EthernetPacket.newPacket(raw_packet, 0, raw_packet.length)
          val ipv4 = eth.get(classOf[IpV4Packet])
          val ipv4h = ipv4.getHeader

          val dip = ipv4h.getDstAddr.toString
          val sip = ipv4h.getSrcAddr.toString
          val proto = ipv4h.getProtocol.toString

          val tcp = ipv4.get(classOf[TcpPacket])
          val tcph = tcp.getHeader
          val dport = tcph.getDstPort.toString
          val sport = tcph.getSrcPort.toString

          (rowkey, dip, sip, dport, sport, proto, raw_packet, BigInt(Array(0.toByte) ++ ts_byte).toString)
        } toOption

        val udp_tuple5 = Try {
          val eth = EthernetPacket.newPacket(raw_packet, 0, raw_packet.length)
          val ipv4 = eth.get(classOf[IpV4Packet])
          val ipv4h = ipv4.getHeader

          val dip = ipv4h.getDstAddr.toString
          val sip = ipv4h.getSrcAddr.toString
          val proto = ipv4h.getProtocol.toString

          val udp = ipv4.get(classOf[UdpPacket])
          val udph = udp.getHeader
          val dport = udph.getDstPort.toString
          val sport = udph.getSrcPort.toString

          (rowkey, dip, sip, dport, sport, proto, raw_packet, BigInt(Array(0.toByte) ++ ts_byte).toString)
        } toOption

        (tcp_tuple5, udp_tuple5) match {
          case (Some(v), _) => Some(v)
          case (_, Some(v)) => Some(v)
          case _ => None
        }
    }

    val tuple5_filter_rdd = tuple5_rdd.filter{
      v =>
        v match {
          case None => false
          case _ => true
        }
    }.map(a => a.get)

    tuple5_filter_rdd
      .toHBaseTable(save_table)
      .toColumns("di", "si", "dp", "sp", "pr", "r", "t")
      .inColumnFamily("p")
      .save()
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
