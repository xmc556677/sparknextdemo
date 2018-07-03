package cc.xmccc.sparkdemo

import java.security.MessageDigest

import cc.xmccc.hbase.util.HBaseUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.pcap4j.core.{PcapHandle, Pcaps}
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy

import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.File
import scala.util.{Failure, Success, Try}


object PcapToHbase {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.master", "hmaster.hbase:60000")
  val conn = ConnectionFactory.createConnection(conf)
  val admin = conn.getAdmin()

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("PutPcapToHbase")
      .getOrCreate()

    val filepath = args(0)
    val maxbufsize = args(1).toInt
    val save_table = args(2)

    val pcap = Pcaps.openOffline(filepath)
    val filesize = File(args(0)).length

    if (! admin.tableExists(TableName.valueOf(save_table)))
      createPresplitTable(save_table)

    val it = new MaxLengthPacketIterator(pcap, maxbufsize)
    for( packetinfos <- it) {
      println("declare boradcast")
      println("length of packetinfo: " + packetinfos.length)
      val broad = sparkSession.sparkContext.broadcast(packetinfos)
      val rdd = sparkSession.sparkContext.parallelize(0 to broad.value.length-1, 32)
      val packetrdd = rdd.map(i => broad.value(i))
      val hbaserdd = packetrdd.map {
        case(ts_sec, ts_usec, orig_len, raw_packet) => {
          val ts_ms = BigInt(ts_sec) * 1000000 + BigInt(ts_usec)
          val ts_ms_bs = Bytes.toBytes(ts_ms.toLong)

          val rowkey = MessageDigest.getInstance("MD5").digest(ts_ms_bs)
          (rowkey, raw_packet, ts_ms_bs)
        }
      }
      hbaserdd.toHBaseTable(save_table)
          .toColumns("r", "t")
          .inColumnFamily("p")
          .save()
      broad.destroy()
    }
  }

  class MaxLengthPacketIterator(val pcapHandle: PcapHandle, val max_size: Int) extends Iterator[Array[(Int, Int, Int, Array[Byte])]] {
    type PacketInfo = (Int, Int, Int, Array[Byte])
    var isend = false

    override def hasNext: Boolean = !isend

    override def next(): Array[(Int, Int, Int, Array[Byte])] = {
      var length = 0
      val arrayBuff = ArrayBuffer.empty[PacketInfo]
      while(length < max_size && (! isend)) {
        Try{pcapHandle.getNextRawPacketEx} match {
          case Success(p) => {
            length += p.length
            arrayBuff.append((
              pcapHandle.getTimestamp.getTime / 1000 toInt,
              pcapHandle.getTimestamp.getNanos / 1000 toInt,
              pcapHandle.getOriginalLength,
              p
            ))
          }
          case Failure(p) => {
            isend = true
          }
        }
      }
      arrayBuff.toArray
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
