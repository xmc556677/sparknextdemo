package cc.xmccc.sparkdemo

import cc.xmccc.hbase.util.HBaseUtil
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import org.apache.spark.sql.SparkSession
import org.pcap4j.packet.namednumber.IpNumber
import org.pcap4j.packet.{EthernetPacket, IpV4Packet, TcpPacket}

object ExtractPacketFeature {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.master", "hmaster.hbase:60000")
  val conn = ConnectionFactory.createConnection(conf)
  val admin = conn.getAdmin()

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("ExtractSessionFeature")
      .getOrCreate()

    val input_table = "xmc:sessions_2gb"
    val save_table = "xmc:pkt_feature_2gb"

    val input_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Array[Byte], Array[Byte], Array[Byte])](input_table)
      .select("sid", "t", "r" )
      .inColumnFamily("p")

    if (! admin.tableExists(TableName.valueOf(save_table)))
      createPresplitTable(save_table)

    val save_rdd = input_rdd.map{
      case(rowkey, sid, ts, rawpkt) =>
        val pkt = EthernetPacket.newPacket(rawpkt, 0, rawpkt.length)
        val ipv4 = pkt.get(classOf[IpV4Packet])
        val ipv4h = ipv4.getHeader
        val ttl = Array(ipv4h.getTtl)
        val tos = Array(ipv4h.getTos.value)

        if(ipv4h.getProtocol == IpNumber.TCP) {
          val tcp = pkt.get(classOf[TcpPacket])
          val tcph = tcp.getHeader

          val syn = tcph.getSequenceNumber
          val wnd_size = tcph.getWindow
          Some(rowkey, sid, ts, wnd_size, syn, ttl, tos)
        } else {
          None
        }
    }.filter{
      _ match {
        case None => false
        case _ => true
      }
    }.map(_.get)

    save_rdd
      .toHBaseTable(save_table)
      .toColumns("sid", "t", "tcp_wnd_size", "tcp_syn", "ipv4_ttl", "ipv4_tos")
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
