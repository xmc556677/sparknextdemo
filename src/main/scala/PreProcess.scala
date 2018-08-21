package cc.xmccc.sparkdemo

import java.security.MessageDigest

import cc.xmccc.hbase.util.HBaseUtil
import cc.xmccc.sparkdemo.schema.HBaseOpsUtil._
import cc.xmccc.sparkdemo.schema.{NewSessionFeatureTable, ProtoModelTable, SessionFeatureTable}
import org.apache.spark.sql.SparkSession
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.pcap4j.packet.{IpV4Packet, Packet, TcpPacket, UdpPacket}
import org.pcap4j.packet.factory.PacketFactories
import org.pcap4j.packet.namednumber.{DataLinkType, IpNumber}

import scala.util.Try

object PreProcess {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.master", "hmaster.hbase:60000")
  val conn = ConnectionFactory.createConnection(conf)
  val admin = conn.getAdmin()

  def UdpSessionExtract(tuple5_rdd: RDD[(Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte])]) = {
    val udp_format_rdd = tuple5_rdd map {
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

    sessions_list_rdd flatMap {x => x._1}
  }

  def TcpSessionExtract(tuple5_rdd: RDD[(Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte])]) = {
    val tcp_format_rdd = tuple5_rdd.map{
      case (rowkey, dip, sip, dport, sport, proto, ts_b, rawpacket, mark_b) =>
        val mark_n = BigInt(mark_b)
        val ts = BigInt(Array(0.toByte) ++ ts_b)

        Try {
          val ethp = parsePacket(rawpacket)
          val tcpp = ethp.get(classOf[TcpPacket])
          val tcph = tcpp.getHeader
          val flags = tcpp.getRawData.slice(13, 14)(0)
          val ackn = tcph.getAcknowledgmentNumberAsLong
          val synn = tcph.getSequenceNumberAsLong
          (mark_n, mark_b, ts, flags, ackn, synn, (BigInt(dip), BigInt(dport)), (BigInt(sip), BigInt(sport)), rowkey, rawpacket, ts_b)
        } toOption
    }.filter{
      case None => false
      case _ => true
    }.map(_.get)

    val grouped = tcp_format_rdd.groupBy(_._1)

    val sessions_list_rdd = tcp_format_rdd.groupBy(_._1)
      .map{
        case(_, it) =>
          val items = it.toList sortBy (x => x._3)
          val tcp_seq = items filter {
            case(_, _, _, flags, _, _, _, _, _, rawpacket, ts_b) =>
              if(flags == 0x02 ||
                flags == 0x12 ||
                flags == 0x10) {
                true
              } else {
                false
              }
          }

          val stop_seq = items filter {
            case(_, _, _, flags, _, _, _, _, _, _, _) =>
              if ((flags & 0x1) != 0 || (flags & 0x4) != 0 ) {
                true
              } else {
                false
              }
          } map (x => x._3)

          var flags_cache = scala.collection.mutable.HashMap.empty[(Byte, Long, Long, (BigInt, BigInt), (BigInt, BigInt)), BigInt]

          val handshakes_ts_seq = tcp_seq.foldLeft[List[BigInt]](Nil){
            case(result, (_, mark_b, ts, flags, ackn, synn, dest, src, _, _, _)) =>
              val item = (flags, ackn, synn, dest, src)
              val r: Option[BigInt] = flags match {
                case 0x002 => {
                  flags_cache.put(item, ts)
                  None
                }
                case 0x012 => {
                  val last = flags_cache.get(0x002.toByte, 0, ackn - 1, src, dest)
                  if( last != None) {
                    flags_cache.put(item, last.get)
                  }
                  None
                }
                case 0x010 => {
                  val last = flags_cache.get(0x012.toByte, synn, ackn - 1, src, dest)
                  if(last != None) {
                    last
                  } else {
                    None
                  }
                }
              }

              r match {
                case Some(v) => v :: result
                case _ => result
              }
          } distinct

          val tcp_mark_seq = (
            (handshakes_ts_seq map (x => (0, x))) ++ (stop_seq map (x => (1, x))) ) sortBy (_._2)

          val result = items.foldLeft((List.empty[(Array[Byte], Option[Array[Byte]], Array[Byte], Array[Byte])], tcp_mark_seq)){
            case((result, ts_seq), item) =>
              val rowkey = item._9
              val ts = item._3
              val mark_b = item._2
              val ts_b = item._11
              val rawpacket = item._10

              ts_seq match {
                case Nil =>
                  ((rowkey, None, ts_b, rawpacket) :: result, ts_seq)
                case (_, first) :: _ if (ts < first) =>
                  ((rowkey, None, ts_b, rawpacket) :: result, ts_seq)
                case (1, _) :: (0, first) :: _ if first == ts =>
                  ((rowkey, Some(mark_b ++ ensureXByte(first.toByteArray, 8)), ts_b, rawpacket) :: result, ts_seq.drop(1))
                case (1, _) :: _ =>
                  ((rowkey, None, ts_b, rawpacket) :: result, ts_seq.drop(1))
                case (0, first) :: Nil if ts >= first =>
                  ((rowkey, Some(mark_b ++ ensureXByte(first.toByteArray, 8)), ts_b, rawpacket) :: result, ts_seq)


                case (0, first) :: (1, second) :: _ if (ts >= first && ts < second) =>
                  ((rowkey, Some(mark_b ++ ensureXByte(first.toByteArray, 8)), ts_b, rawpacket) :: result, ts_seq)
                case (0, first) :: (1, second) :: _  =>
                  ((rowkey, None, ts_b, rawpacket) :: result, ts_seq.drop(1))

                case (0, first) :: (0, second) :: _ if (ts >= first && ts < second) =>
                  ((rowkey, Some(mark_b ++ ensureXByte(first.toByteArray, 8)), ts_b, rawpacket) :: result, ts_seq)
                case (0, first) :: (0, second) :: _ =>
                  ((rowkey, Some(mark_b ++ ensureXByte(second.toByteArray, 8)), ts_b, rawpacket) :: result, ts_seq.drop(1))
              }
          }

          result._1.filter(_._2 != None).map(x => (x._1, x._2.get, x._3, x._4))
      }

    sessions_list_rdd.flatMap(x => x).map(item => (item._1, item._2))
  }

  def Tuple5Extract(rawpkt_rdd: RDD[(Array[Byte], Array[Byte], Array[Byte])]): RDD[(Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte])] = {
    val tuple5_rdd = rawpkt_rdd.map{
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
  }

  def Tuple5MarkExtract(tuple5_rdd: RDD[(Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte])]): RDD[(Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte])]= {
    val save_rdd = tuple5_rdd.map{
      case(rowkey, dip_b, sip_b, dport_b, sport_b, proto_b, rawpacket, ts_b) =>
        val destination_b = dip_b ++ dport_b
        val source_b = sip_b ++ sport_b
        val first :: second :: Nil = List(destination_b, source_b).sortBy(x => BigInt(x))
        val tuple5_b = first ++ second ++ proto_b

        (rowkey, dip_b, sip_b, dport_b, sport_b, proto_b, ts_b, rawpacket, tuple5_b)
    }

    save_rdd
  }

  def TcpPktFeatureExtract(rawpkt_rdd: RDD[(Array[Byte], Array[Byte], Array[Byte])]): RDD[(Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte])] = {
    val save_rdd = rawpkt_rdd.map{
      case(rowkey, rawpkt, ts) =>
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
          val payload = Try{
            tcp.getPayload.getRawData
          } getOrElse(Array(0.toByte))
          val payload_len_b = Bytes.toBytes(payload_len)

          (rowkey, dport_b, sport_b, proto,flags_b, pkt_len_b, payload_len_b, payload)
        } toOption
    }.filter{
      case None => false
      case _ => true
    }.map{_.get}

    save_rdd
  }

  def SessionFeatureExtract(pkts_rdd: RDD[(Array[Byte], Option[Array[Byte]], Array[Byte], Array[Byte])]): RDD[NewSessionFeatureTable] = {

    val sid_rdd = pkts_rdd.filter(_._2 != None).map(x => (x._1, x._2.get, x._3, x._4)).map{
      case(_, sid, ts, rawpacket) =>
        val sid_md5_b = MessageDigest.getInstance("MD5").digest(sid)
        val sid_md5_n = BigInt(sid_md5_b)

        (sid_md5_n, sid_md5_b, ts, rawpacket)
    }

    val sid_group_rdd = sid_rdd.groupBy(_._1).cache()

    val save_rdd = sid_group_rdd.map{
      case(_, sessn_datas) =>
        val ds = sessn_datas.toList
        val datas_ = ds
          .sortBy(x => BigInt(Array(0.toByte) ++ x._3))
          .map{
            case (_, _, ts_b, rawpkt) =>
              (BigInt(Array(0.toByte) ++ ts_b), parsePacket(rawpkt), rawpkt)
          }
        val datas = ds.sortBy(x => BigInt(Array(0.toByte) ++ x._3))
        val rowkey = datas(0)._2
        val sid = datas(0)._2
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
        val var_pkt_len = raw_pkt_lens.map(x => math.pow(x - avg_pkt_len, 2)).sum / avg_pkt_len.toDouble

        val avg_ts_IAT = ts_IAT.sum / ts_IAT.length
        val min_ts_IAT = ts_IAT.min
        val max_ts_IAT = ts_IAT.max
        val var_ts_IAT = (BigDecimal(ts_IAT.map(x => math.pow((x - avg_ts_IAT).toDouble, 2)).sum) / ts_IAT.length).toDouble

        val avg_pld_len = payload_lens.sum / payload_lens.length
        val min_pld_len = payload_lens.min
        val max_pld_len = payload_lens.max
        val var_pld_len = (BigDecimal(payload_lens.map(x => math.pow(x - avg_pld_len, 2)).sum) / payload_lens.length).toDouble

        val ttl_bytes = raw_pkt_lens.map(_.toLong).sum

        val sessn_dur = tss.max - tss.min
        val pkg_cnt = datas.length
        val psh_cnt = pkts.map(extract_tcp)
          .map{x => x.map(tcpp => tcpp.getHeader.getPsh)}
          .count{
            case Some(v) => v
            case _ => false
          }

        val statistic_datas = datas_
          .map{x =>
            val direction = get_direction(x._2).map{
              case ((sip, sport), (dip, dport), _) =>
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

        val cs_datas = statistic_datas.filter{
          case(_, _, Some(direction), _) =>
            direction == cs.get.get
          case _ => false
        }

        val (cs_avg_pkt_len, cs_min_pkt_len, cs_max_pkt_len, cs_var_pkt_len,
        cs_avg_ts_IAT, cs_min_ts_IAT, cs_max_ts_IAT, cs_var_ts_IAT,
        cs_avg_pld_len, cs_min_pld_len, cs_max_pld_len, cs_var_pld_len,
        cs_ttl_bytes, cs_pkt_cnt) = stream_statitic_pattern(cs_datas)

        val (sc_avg_pkt_len, sc_min_pkt_len, sc_max_pkt_len, sc_var_pkt_len,
        sc_avg_ts_IAT, sc_min_ts_IAT, sc_max_ts_IAT, sc_var_ts_IAT,
        sc_avg_pld_len, sc_min_pld_len, sc_max_pld_len, sc_var_pld_len,
        sc_ttl_bytes, sc_pkt_cnt) = stream_statitic_pattern(sc_datas)

        val Some(((sip, sport), (dip, dport), proto)) :: Nil = pkts.take(1).map(get_direction)

        val direction = sip ++ sport ++ dip ++ dport ++ Array(proto.toByte)

        val mark = dip ++ dport ++ Array(proto.toByte)

        val features_name = Array(
          "avg_pkt_len", "min_pkt_len", "max_pkt_len", "var_pkt_len",
          "avg_ts_IAT", "min_ts_IAT", "max_ts_IAT", "var_ts_IAT",
          "avg_pld_len", "min_pld_len", "max_pld_len", "var_pld_len",
          "total_bytes", "sessn_dur", "pkts_cnt", "psh_cnt",
          "sc_avg_pkt_len", "sc_min_pkt_len", "sc_max_pkt_len", "sc_var_pkt_len",
          "sc_avg_ts_IAT", "sc_min_ts_IAT", "sc_max_ts_IAT", "sc_var_ts_IAT",
          "sc_avg_pld_len", "sc_min_pld_len", "sc_max_pld_len", "sc_var_pld_len",
          "sc_total_bytes", "sc_pkt_cnt",
          "cs_avg_pkt_len", "cs_min_pkt_len", "cs_max_pkt_len", "cs_var_pkt_len",
          "cs_avg_ts_IAT", "cs_min_ts_IAT", "cs_max_ts_IAT", "cs_var_ts_IAT",
          "cs_avg_pld_len", "cs_min_pld_len", "cs_max_pld_len", "cs_var_pld_len",
          "cs_total_bytes", "cs_pkt_cnt"
        )

        val features_value: Array[Double] = Array(
          avg_pkt_len, min_pkt_len, max_pkt_len, var_pkt_len,
          avg_ts_IAT.toDouble, min_ts_IAT.toDouble, max_ts_IAT.toDouble, var_ts_IAT.toDouble,
          avg_pld_len, min_pld_len, max_pld_len, var_pld_len,
          ttl_bytes, sessn_dur.toDouble, pkg_cnt, psh_cnt,
          sc_avg_pkt_len, sc_min_pkt_len, sc_max_pkt_len, sc_var_pkt_len,
          sc_avg_ts_IAT.toDouble, sc_min_ts_IAT.toDouble, sc_max_ts_IAT.toDouble, sc_var_ts_IAT.toDouble,
          sc_avg_pld_len, sc_min_pld_len, sc_max_pld_len, sc_var_pld_len,
          sc_ttl_bytes, sc_pkt_cnt,
          cs_avg_pkt_len, cs_min_pkt_len, cs_max_pkt_len, cs_var_pkt_len,
          cs_avg_ts_IAT.toDouble, cs_min_ts_IAT.toDouble, cs_max_ts_IAT.toDouble, cs_var_ts_IAT.toDouble,
          cs_avg_pld_len, cs_min_pld_len, cs_max_pld_len, cs_var_pld_len,
          cs_ttl_bytes, cs_pkt_cnt
        )

        NewSessionFeatureTable(
          rowkey, sport, dport, direction, mark, sid, features_name, features_value
        )
    }
    save_rdd
  }

  def PktDirectionAndFuzzySetMarkExtract(tuple5_sid_rdd: RDD[(Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Option[Array[Byte]])]): RDD[(Array[Byte], String, Array[Byte])] = {
    val saved_rdd = tuple5_sid_rdd.filter(_._8 != None).groupBy(x => BigInt(x._8.get))
      .flatMap{
        case(_, it) =>
          val items = it.toList.sortBy(x => BigInt(Array(0.toByte) ++ x._7))
          val direction_seq = items.map{
            case(r, si, sp, di, dp, proto, _, _) =>
              val src = BigInt(si ++ sp)
              val dst = BigInt(di ++ dp)
              (r, (src, dst))
          }

          val (_, _, _, di, dp, proto, _, _) = items(0)
          val fuzzyset_mark = di ++ dp ++ proto

          val forward = direction_seq(0)
          direction_seq.map{
            case(r, direction) =>
              if(direction == forward._2)
                (r, "forward", fuzzyset_mark)
              else
                (r, "back", fuzzyset_mark)
          }
      }

    saved_rdd
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("PreProcess")
      .getOrCreate()

    val input_table = args(0)
    val save_table = args(1)
    val save_table2 = args(2)

    val input_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Array[Byte], Array[Byte])](input_table)
      .select( "r", "t")
      .inColumnFamily("p")

    val tuple5_rdd = Tuple5Extract(input_rdd)
    val tuple5_mark_rdd = Tuple5MarkExtract(tuple5_rdd)
    val packet_feature_rdd = TcpPktFeatureExtract(input_rdd)

    if (! admin.tableExists(TableName.valueOf(save_table)))
      createPresplitTable(save_table, "p")
    if (! admin.tableExists(TableName.valueOf(save_table2)))
      createPresplitTable(save_table2, "sessn")

    val udp_sids = UdpSessionExtract(tuple5_mark_rdd)
    val tcp_sids = TcpSessionExtract(tuple5_mark_rdd)

    tuple5_mark_rdd.toHBaseTable(save_table)
        .toColumns("di", "si", "dp", "sp", "pr", "t", "r", "fm")
        .inColumnFamily("p")
        .save
    packet_feature_rdd.toHBaseTable(save_table)
        .toColumns("dport", "sport", "proto", "tcp_flags", "pkt_len", "pld_len", "payload")
        .inColumnFamily("p")
        .save
    udp_sids.toHBaseTable(save_table)
        .toColumns("sid")
        .inColumnFamily("p")
        .save
    tcp_sids.toHBaseTable(save_table)
        .toColumns("sid")
        .inColumnFamily("p")
        .save

    val pkts_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Option[Array[Byte]], Array[Byte], Array[Byte])](save_table)
      .select("sid", "t", "r" )
      .inColumnFamily("p")

    val tuple5_sid_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Option[Array[Byte]])](save_table)
      .select("si", "sp", "di", "dp", "pr", "t", "sid")
      .inColumnFamily("p")


    val pkt_direction_m_rdd = PktDirectionAndFuzzySetMarkExtract(tuple5_sid_rdd)

    pkt_direction_m_rdd.toHBaseTable(save_table)
      .toColumns("direction", "m")
      .inColumnFamily("p")
      .save()

    val sessn_feature_rdd = SessionFeatureExtract(pkts_rdd)


    sessn_feature_rdd.toHBaseTable(save_table2)
      .toColumns(
        "sport", "dport", "direction", "m", "sid", "features_name", "features_value"
      )
      .inColumnFamily("sessn")
      .save()

    println(s"TCP session count: ${tcp_sids.count}")
    println(s"UDP session count: ${udp_sids.count}")
    println(s"ICMP session count: 0")

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

  def stream_statitic_pattern(datas: List[(BigInt, Int, Option[(BigInt, BigInt)], Option[Int])]) = {
    val pkt_len = datas.map(_._2)
    val avg_pkt_len = pkt_len.sum / pkt_len.length
    val min_pkt_len = pkt_len.min
    val max_pkt_len = pkt_len.max
    val var_pkt_len = pkt_len.map(x => (x - avg_pkt_len) ^ 2).sum / pkt_len.length.toDouble

    val ts_seq = datas.map(_._1)
    val ts_IAT = (ts_seq zip ts_seq.drop(1)) map {case(x, y) => y - x}
    val avg_ts_IAT = Try{ts_IAT.sum / ts_IAT.length}.getOrElse(BigInt(0))
    val min_ts_IAT = Try{ts_IAT.min}.getOrElse(BigInt(0))
    val max_ts_IAT = Try{ts_IAT.max}.getOrElse(BigInt(0))
    val var_ts_IAT = Try{BigDecimal(ts_IAT.map(x => math.pow((x - avg_ts_IAT).toDouble, 2)).sum) / ts_IAT.length}.getOrElse(BigDecimal(0)).toDouble

    val pld_len = datas.map(_._4).filter(_ != None).map(_.get)
    val avg_pld_len = Try{pld_len.sum / pld_len.length}.getOrElse(0)
    val min_pld_len = Try{pld_len.min}.getOrElse(0)
    val max_pld_len = Try{pld_len.max}.getOrElse(0)
    val var_pld_len = Try{BigDecimal(pld_len.map(x => math.pow(x - avg_pld_len, 2)).sum) / pld_len.length}.getOrElse(BigDecimal(0)).toDouble

    val ttl_bytes = pkt_len.sum
    val pkt_cnt = pkt_len.length

    (avg_pkt_len, min_pkt_len, max_pkt_len, var_pkt_len,
      avg_ts_IAT, min_ts_IAT, max_ts_IAT, var_ts_IAT,
      avg_pld_len, min_pld_len, max_pld_len, var_pld_len,
      ttl_bytes, pkt_cnt)
  }

  def get_direction(pkt: Packet): Option[((Array[Byte], Array[Byte]), (Array[Byte], Array[Byte]), Byte)] = {
    (pkt.contains(classOf[TcpPacket]), pkt.contains(classOf[IpV4Packet])) match {
      case (true, true) => {
        val ipv4h = pkt.get(classOf[IpV4Packet]).getHeader
        val tcph = pkt.get(classOf[TcpPacket]).getHeader
        val dest = (ipv4h.getDstAddr.getAddress, Bytes.toBytes(tcph.getDstPort.value))
        val src = (ipv4h.getSrcAddr.getAddress, Bytes.toBytes(tcph.getSrcPort.value))
        val proto = ipv4h.getProtocol.value
        Some(src, dest, proto)
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
      case _ => None
    }
  }

  def parsePacket(rawpacket: Array[Byte]): Packet = {
    PacketFactories.getFactory(classOf[Packet], classOf[DataLinkType])
      .newInstance(rawpacket, 0, rawpacket.length, DataLinkType.EN10MB)
  }

  def createPresplitTable(name: String, cf: String): Unit = {
    val table_name_create_presplit = TableName.valueOf(name)
    val table_presplit = new HTableDescriptor(table_name_create_presplit)
    table_presplit.addFamily(new HColumnDescriptor(cf))
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
