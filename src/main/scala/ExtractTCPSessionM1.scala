package cc.xmccc.sparkdemo

import cc.xmccc.hbase.util.HBaseUtil
import org.apache.spark.sql.SparkSession
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import org.apache.hadoop.hbase.util.Bytes
import org.pcap4j.packet.{EthernetPacket, TcpPacket}

import scala.util.Try

/*
* extract sessions from tcp flow
*
* method 1
* */

object ExtractTCPSessionM1 {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.master", "hmaster.hbase:60000")
  val conn = ConnectionFactory.createConnection(conf)
  val admin = conn.getAdmin()

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("ExtractSession")
      .getOrCreate()

    val input_table = "xmc:tuple5_2gb"
    val save_table = "xmc:sessions_2gb"

    val input_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte])](input_table)
      .select("di", "si", "dp", "sp", "pr", "t", "r", "m" )
      .inColumnFamily("p")

    if (! admin.tableExists(TableName.valueOf(save_table)))
      createPresplitTable(save_table)

    val tcp_format_rdd = input_rdd.map{
      case (rowkey, dip, sip, dport, sport, proto, ts_b, rawpacket, mark_b) =>
        val mark_s = Bytes.toString(mark_b)
        val ts = BigInt(Array(0.toByte) ++ ts_b)

        Try {
          val ethp = EthernetPacket.newPacket(rawpacket, 0, rawpacket.length)
          val tcpp = ethp.get(classOf[TcpPacket])
          val tcph = tcpp.getHeader
          val flags = tcph.getRawData.slice(25, 26)(0)
          val ackn = tcph.getAcknowledgmentNumberAsLong
          val synn = tcph.getSequenceNumberAsLong
          (mark_s, ts, flags, ackn, synn, (Bytes.toString(dip), Bytes.toString(dport)), (Bytes.toString(sip), Bytes.toString(sport)))
        } toOption
    }.filter{
      _ match {
        case None => false
        case _ => true
      }
    }.map(_.get)

    val syn_rdd = tcp_format_rdd.filter(_._3 == 0x002).cache()
    val syn_ack_rdd = tcp_format_rdd.filter(_._3 == 0x012).cache()
    val ack_rdd = tcp_format_rdd.filter(_._3 == 0x010).cache()

    println(syn_rdd.count())
    println(syn_ack_rdd.count())
    println(ack_rdd.count())

    val handshake_rdd = syn_rdd cartesian syn_ack_rdd repartition 30 filter {
      case(syn, syn_ack) =>
        syn._1 == syn_ack._1 &&
          syn._2 <= syn_ack._2 &&
          syn._6 == syn_ack._7 &&
          syn._4 + 1 == syn_ack._4
    } cartesian ack_rdd repartition 30 filter {
      case((syn, syn_ack), ack) =>
        //same mark
        syn._1 == syn_ack._1 &&
          syn._1 == ack._1 &&
        //validate timestamp sequence
          syn._2 <= syn_ack._2 &&
          syn_ack._2 <= ack._2 &&
        //validate direction
          (syn._6 == syn_ack._7 &&
             syn._6 == ack._6) &&
        //validate seq number
          syn._4 + 1 == syn_ack._4 &&
          syn_ack._5 + 1 == ack._5
    } map {
      case((syn, _), _) =>
        (syn._1, syn._2)
    } groupBy {
      _._1
    } map {
      x => (x._1, x._2.toList.map(_._2).sorted(Ordering[BigInt].reverse))
    }

    println(handshake_rdd.count())

    val handshake_map = handshake_rdd.collect.toMap
    val broadmap = sparkSession.sparkContext.broadcast(handshake_map)

    val saved_rdd = input_rdd.map(x => (x._1, x._9, x._7, x._8)).map{
      case (rowkey, mark_b, ts_b, rawpacket) =>
        val mark_s = Bytes.toString(mark_b)
        val ts = BigInt(Array(0.toByte) ++ ts_b)

        Try {
          val tss = broadmap.value.get(mark_s).get
          val ts_mark = tss.filter(x => x <= ts)(0)
          val ts_mark_b = ensureXByte(ts_mark.toByteArray, 8)
          (rowkey, ts_b, rawpacket, mark_b ++ ts_mark_b)
        } toOption
    }.filter{
      _ match {
        case None => false
        case _ => true
      }
    }.map(_.get)

    saved_rdd
      .toHBaseTable(save_table)
      .toColumns("t", "r", "sid")
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


