package cc.xmccc.sparkdemo

import java.security.MessageDigest

import cc.xmccc.hbase.util.HBaseUtil
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import org.apache.spark.sql.SparkSession
import it.nerdammer.spark.hbase._
import it.nerdammer.spark.hbase.conversion.{FieldWriter, SingleColumnFieldWriter}
import org.apache.hadoop.hbase.util.Bytes
import org.pcap4j.packet.{IpV4Packet, Packet, TcpPacket}
import org.pcap4j.packet.factory.PacketFactories
import org.pcap4j.packet.namednumber.DataLinkType

import scala.util.Try


/*
* TODO improve code readability and structue
* */

object ExtractSessionFeature {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.master", "hmaster.hbase:60000")
  val conn = ConnectionFactory.createConnection(conf)
  val admin = conn.getAdmin()

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("ExtractSessionFeature")
      .getOrCreate()

    val input_table = args(0)
    val save_table = args(1)

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

    val sid_group_rdd = sid_rdd.groupBy(_._1).cache()

    val save_rdd1 = sid_group_rdd.map{
      case(_, sessn_datas) =>
        val datas = sessn_datas.toList.sortBy(x => BigInt(Array(0.toByte) ++ x._3))
        val rowkey = datas(0)._2
        val tss = datas.map(x => BigInt(Array(0.toByte) ++ x._3))
        val ts_IAT = (tss zip tss.drop(1)) map {case (x, y) => y - x}
        val raw_pkt_lens = datas.map(_._4.length)
        val pkts = datas.map(x => parsePacket(x._4))
        val payload_lens = pkts.map(extract_payload).map{
          case Some(p) => p.getRawData.length
          case _ => 0
        }

        val avg_pkt_len = raw_pkt_lens.sum / raw_pkt_lens.length
        val min_pkt_len = raw_pkt_lens.min
        val max_pkt_len = raw_pkt_lens.max
        val var_pkt_len = raw_pkt_lens.map(x => (x - avg_pkt_len) ^ 2).sum / avg_pkt_len

        val avg_ts_IAT = ts_IAT.sum / ts_IAT.length
        val min_ts_IAT = ts_IAT.min
        val max_ts_IAT = ts_IAT.max
        val var_ts_IAT = ts_IAT.map(x => (x - avg_ts_IAT) ^ 2).sum / ts_IAT.length

        val avg_pld_len = payload_lens.sum / payload_lens.length
        val min_pld_len = payload_lens.min
        val max_pld_len = payload_lens.max
        val var_pld_len = payload_lens.map(x => (x - avg_pld_len) ^ 2).sum / payload_lens.length

        val ttl_bytes = raw_pkt_lens.map(_.toLong).sum

        val sessn_dur = tss.max - tss.min
        val pkg_cnt = datas.length
        val psh_cnt = pkts.map(extract_tcp)
          .map{x => x.map(tcpp => tcpp.getHeader.getPsh)}
          .count{
            case Some(v) => v
            case _ => false
          }

        val Some(((_, sport), (_, dport))) :: Nil = pkts.take(1).map(get_direction)

        (rowkey, avg_pkt_len, min_pkt_len, max_pkt_len, var_pkt_len,
          avg_ts_IAT, min_ts_IAT, max_ts_IAT, var_ts_IAT,
          avg_pld_len, min_pld_len, max_pld_len, var_pld_len,
          ttl_bytes, sessn_dur, pkg_cnt, psh_cnt, sport, dport)
    }

    implicit def bigintWriter: FieldWriter[BigInt] = new SingleColumnFieldWriter[BigInt] {
      override def mapColumn(data: BigInt): Option[Array[Byte]] = Some(ensureXByte(data.toByteArray, 8))
    }

    save_rdd1
      .toHBaseTable(save_table)
      .toColumns("avg_pkt_len", "min_pkt_len", "max_pkt_len", "var_pkt_len",
        "avg_ts_IAT", "min_ts_IAT", "max_ts_IAT", "var_ts_IAT",
        "avg_pld_len", "min_pld_len", "max_pld_len", "var_pld_len",
        "total_bytes", "sessn_dur", "pkts_cnt", "psh_cnt", "sport", "dport")
      .inColumnFamily("sessn")
      .save()

    val save_rdd2 = sid_group_rdd.map{
      case (_, sessn_datas) =>
        val ds = sessn_datas.toList
        val datas = ds
          .sortBy(x => BigInt(Array(0.toByte) ++ x._3))
          .map{
            case (_, _, ts_b, rawpkt) =>
              (BigInt(Array(0.toByte) ++ ts_b), parsePacket(rawpkt), rawpkt)
          }

        val rowkey = ds(0)._2

        val statistic_datas = datas
          .map{x =>
            val direction = get_direction(x._2).map{
              case ((sip, sport), (dip, dport)) =>
                (BigInt(Array(0.toByte) ++ sip ++ sport),
                  BigInt(Array(0.toByte) ++ dip ++ dport))
            }
            val payload = extract_payload(x._2).map{_.getRawData.length}
            (x._1, x._3.length, direction, payload)
          }

        val cs = statistic_datas.filter{
          case(_, _, Some(_), _) => true
          case _ => false
        }.lift(0).map(_._3)

        val sc_datas = statistic_datas.filter{
          case(_, _, Some(direction), _) =>
            ! (direction == cs.get.get)
          case _ => false
        }

        val (sc_avg_pkt_len, sc_min_pkt_len, sc_max_pkt_len, sc_var_pkt_len,
          sc_avg_ts_IAT, sc_min_ts_IAT, sc_max_ts_IAT, sc_var_ts_IAT,
          sc_avg_pld_len, sc_min_pld_len, sc_max_pld_len, sc_var_pld_len,
          sc_ttl_bytes) = stream_statitic_pattern(sc_datas)


        (rowkey, sc_avg_pkt_len, sc_min_pkt_len, sc_max_pkt_len, sc_var_pkt_len,
          sc_avg_ts_IAT, sc_min_ts_IAT, sc_max_ts_IAT, sc_var_ts_IAT,
          sc_avg_pld_len, sc_min_pld_len, sc_max_pld_len, sc_var_pld_len,
          sc_ttl_bytes)
    }

    save_rdd2
      .toHBaseTable(save_table)
      .toColumns("sc_avg_pkt_len", "sc_min_pkt_len", "sc_max_pkt_len", "sc_var_pkt_len",
        "sc_avg_ts_IAT", "sc_min_ts_IAT", "sc_max_ts_IAT", "sc_var_ts_IAT",
        "sc_avg_pld_len", "sc_min_pld_len", "sc_max_pld_len", "sc_var_pld_len",
        "sc_total_bytes")
      .inColumnFamily("sessn")
      .save()

    val save_rdd3 = sid_group_rdd.map{
      case (_, sessn_datas) =>
        val ds = sessn_datas.toList
        val datas = ds
          .sortBy(x => BigInt(Array(0.toByte) ++ x._3))
          .map{
            case (_, _, ts_b, rawpkt) =>
              (BigInt(Array(0.toByte) ++ ts_b), parsePacket(rawpkt), rawpkt)
          }

        val rowkey = ds(0)._2

        val statistic_datas = datas
          .map{x =>
            val direction = get_direction(x._2).map{
              case ((sip, sport), (dip, dport)) =>
                (BigInt(Array(0.toByte) ++ sip ++ sport),
                  BigInt(Array(0.toByte) ++ dip ++ dport))
            }
            val payload = extract_payload(x._2).map{_.getRawData.length}
            (x._1, x._3.length, direction, payload)
          }

        val cs = statistic_datas.filter{
          case(_, _, Some(_), _) => true
          case _ => false
        }.lift(0).map(_._3)

        val cs_datas = statistic_datas.filter{
          case(_, _, Some(direction), _) =>
            direction == cs.get.get
          case _ => false
        }

        val (cs_avg_pkt_len, cs_min_pkt_len, cs_max_pkt_len, cs_var_pkt_len,
        cs_avg_ts_IAT, cs_min_ts_IAT, cs_max_ts_IAT, cs_var_ts_IAT,
        cs_avg_pld_len, cs_min_pld_len, cs_max_pld_len, cs_var_pld_len,
        cs_ttl_bytes) = stream_statitic_pattern(cs_datas)

        (rowkey, cs_avg_pkt_len, cs_min_pkt_len, cs_max_pkt_len, cs_var_pkt_len,
          cs_avg_ts_IAT, cs_min_ts_IAT, cs_max_ts_IAT, cs_var_ts_IAT,
          cs_avg_pld_len, cs_min_pld_len, cs_max_pld_len, cs_var_pld_len,
          cs_ttl_bytes)
    }

    save_rdd3
      .toHBaseTable(save_table)
      .toColumns("cs_avg_pkt_len", "cs_min_pkt_len", "cs_max_pkt_len", "cs_var_pkt_len",
        "cs_avg_ts_IAT", "cs_min_ts_IAT", "cs_max_ts_IAT", "cs_var_ts_IAT",
        "cs_avg_pld_len", "cs_min_pld_len", "cs_max_pld_len", "cs_var_pld_len",
        "cs_total_bytes")
      .inColumnFamily("sessn")
      .save()

  }

  def parsePacket(rawpacket: Array[Byte]): Packet = {
    PacketFactories.getFactory(classOf[Packet], classOf[DataLinkType])
      .newInstance(rawpacket, 0, rawpacket.length, DataLinkType.EN10MB)
  }

  def stream_statitic_pattern(datas: List[(BigInt, Int, Option[(BigInt, BigInt)], Option[Int])]) = {
    val pkt_len = datas.map(_._2)
    val avg_pkt_len = pkt_len.sum / pkt_len.length
    val min_pkt_len = pkt_len.min
    val max_pkt_len = pkt_len.max
    val var_pkt_len = pkt_len.map(x => (x - avg_pkt_len) ^ 2).sum / pkt_len.length

    val ts_seq = datas.map(_._1)
    val ts_IAT = (ts_seq zip ts_seq.drop(1)) map {case(x, y) => y - x}
    val avg_ts_IAT = ts_IAT.sum / ts_IAT.length
    val min_ts_IAT = ts_IAT.min
    val max_ts_IAT = ts_IAT.max
    val var_ts_IAT = ts_IAT.map(x => (x - avg_ts_IAT) ^ 2).sum / ts_IAT.length

    val pld_len = datas.map(_._4).filter(_ != None).map(_.get)
    val avg_pld_len = Try{pld_len.sum / pld_len.length}.getOrElse(0)
    val min_pld_len = Try{pld_len.min}.getOrElse(0)
    val max_pld_len = Try{pld_len.max}.getOrElse(0)
    val var_pld_len = Try{pld_len.map(x => (x - avg_pld_len) ^ 2).sum / pld_len.length}.getOrElse(0)

    val ttl_bytes = pld_len.sum
    (avg_pkt_len, min_pkt_len, max_pkt_len, var_pkt_len,
    avg_ts_IAT, min_ts_IAT, max_ts_IAT, var_ts_IAT,
    avg_pld_len, min_pld_len, max_pld_len, var_pld_len,
    ttl_bytes)
  }

  def get_direction(pkt: Packet): Option[((Array[Byte], Array[Byte]), (Array[Byte], Array[Byte]))] = {
    (pkt.contains(classOf[TcpPacket]), pkt.contains(classOf[IpV4Packet])) match {
      case (true, true) => {
        val ipv4h = pkt.get(classOf[IpV4Packet]).getHeader
        val tcph = pkt.get(classOf[TcpPacket]).getHeader
        val dest = (ipv4h.getDstAddr.getAddress, Bytes.toBytes(tcph.getDstPort.value))
        val src = (ipv4h.getSrcAddr.getAddress, Bytes.toBytes(tcph.getSrcPort.value))
        Some(src, dest)
      }
      case _ =>  None
    }
  }

  def extract_tcp(pkt: Packet): Option[TcpPacket] = {
    pkt.contains(classOf[TcpPacket]) match {
      case true => Some(pkt.get(classOf[TcpPacket]))
      case _ => None
    }
  }

  def extract_payload(pkt: Packet): Option[Packet] = {
    pkt.contains(classOf[TcpPacket]) match {
      case true => {
        val tcp = pkt.get(classOf[TcpPacket])
        if (tcp.getPayload == null) {
          None
        } else {
          Some(tcp.getPayload)
        }
      }
    }
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
