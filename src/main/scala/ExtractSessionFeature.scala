package cc.xmccc.sparkdemo

import java.security.MessageDigest

import cc.xmccc.hbase.util.HBaseUtil
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import org.apache.spark.sql.SparkSession
import it.nerdammer.spark.hbase._

object ExtractSessionFeature {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.master", "hmaster.hbase:60000")
  val conn = ConnectionFactory.createConnection(conf)
  val admin = conn.getAdmin()

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("ExtractSessionFeature")
      .getOrCreate()

    val input_table = "xmc:sessions_2gb"
    val save_table = "xmc:sessn_feature_2gb"

    val input_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Array[Byte], Array[Byte], Array[Byte])](input_table)
      .select("sid", "t", "r" )
      .inColumnFamily("p")

    if (! admin.tableExists(TableName.valueOf(save_table)))
      createPresplitTable(save_table)

    val sid_rdd = input_rdd.map{
      case(_, sid, ts, rawpacket) =>
        val sid_md5_b = MessageDigest.getInstance("MD5").digest(sid)
        val sid_md5_s = sid_md5_b.mkString("")

        (sid_md5_s, sid_md5_b, ts, rawpacket)
    }

    val save_rdd = sid_rdd.groupBy(_._1).map{
      case(_, session_datas) =>
        val datas = session_datas.toList
        val first :: _ = datas
        val rowkey = first._2
        val tss = datas.map(x => BigInt(Array(0.toByte) ++ x._3))
        val pkts = datas.map(_._4)
        val pkt_lens = pkts.map(_.length)

        val avg_pkt_len = pkt_lens.sum / pkt_lens.length
        val min_pkt_len = pkt_lens.min
        val max_pkt_len = pkt_lens.max
        val sessn_dur = tss.max - tss.min
        val pkg_cnt = pkts.length

        (rowkey, avg_pkt_len, min_pkt_len, max_pkt_len, ensureXByte(sessn_dur.toByteArray, 4), pkg_cnt)
    }

    save_rdd
      .toHBaseTable(save_table)
      .toColumns("avg_pkt_len", "min_pkt_len", "max_pkt_len", "sessn_dur", "pkts_cnt")
      .inColumnFamily("sessn")
      .save()

  }

  def ensureXByte(v: Array[Byte], x: Int): Array[Byte] = {
    val len = v.length
    if(len < x) {
      Array.fill(x - len)(0.toByte) ++ v
    } else if (len > x) {
      v.drop(1)
    } else {
      v
    }
  }

  def createPresplitTable(name: String): Unit = {
    val table_name_create_presplit = TableName.valueOf(name)
    val table_presplit = new HTableDescriptor(table_name_create_presplit)
    table_presplit.addFamily(new HColumnDescriptor("sessn"))
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
