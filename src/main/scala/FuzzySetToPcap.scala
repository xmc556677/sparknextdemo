package cc.xmccc.sparkdemo

import java.io.{BufferedOutputStream, FileOutputStream}

import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession


object FuzzySetToPcap {

  val PCAP_HEADER: Array[Byte] =
    Array(0xd4, 0xc3, 0xb2, 0xa1, 0x02, 0x00, 0x04, 0x00,
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
          0x00, 0x00, 0x40, 0x00, 0x01, 0x00, 0x00, 0x00).map(_.toByte)

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("SessionToPcap")
      .getOrCreate()

    val table = args(0)
    val output_name = args(1)
    val op_id = args(2)
    val op_id_b = (0 to op_id.length-1 by 2).map{
      i =>
        val hex = op_id.slice(i, i+2)
        Integer.parseInt(hex, 16).toByte
    }.toArray

    val packet_table = sparkSession.sparkContext.hbaseTable[(Array[Byte], Array[Byte], Array[Byte], Option[Array[Byte]])](table)
      .select("r", "t", "m")
      .inColumnFamily("p")
      .filter{
        _ match {
          case(_, _, _, Some(_)) => true
          case _ => false
        }
      }.map(item => (item._1, item._2, item._3, item._4.get))

    println(packet_table.count())

    val op_id_b_broadcast = sparkSession.sparkContext.broadcast(op_id_b)

    val result_collect = packet_table.filter{
      case(_, _, _, m) =>
        BigInt(m) == BigInt(op_id_b)
    } map { row => (row._2, row._3) } collect

    println(result_collect.length)

    val ts_uts_raw_pkt = result_collect.map{
      case (pkt, ts_b) =>
        val ts = BigInt(ts_b)
        val ts_usec = ts % 1000000
        val ts_sec = ts / 1000000

        (ts, ts_sec.toInt, ts_usec, pkt)
    }

    val bos = new BufferedOutputStream(new FileOutputStream(output_name))
    bos.write(PCAP_HEADER)

    ts_uts_raw_pkt.sortBy(_._1).foreach{
      case(_, ts_sec, ts_usec, pkt) =>
        //val ts_sec_b = Bytes.toBytes(ts_sec.toLong & 0x00000000ffffffffL).slice(4, 8)
        //val ts_usec_b = Bytes.toBytes(ts_usec.toLong & 0x00000000ffffffffL).slice(4, 8)
        val ts_sec_b = Bytes.toBytes(ts_sec.toInt).reverse
        val ts_usec_b = Bytes.toBytes(ts_usec.toInt).reverse
        val len = pkt.length
        val incl_len_b = Bytes.toBytes(len.toLong & 0x00000000ffffffffL).slice(4, 8).reverse
        val orig_len_b = incl_len_b

        bos.write(ts_sec_b)
        bos.write(ts_usec_b)
        bos.write(incl_len_b)
        bos.write(orig_len_b)
        bos.write(pkt)
    }

    bos.close()
    sparkSession.close()
  }
}
