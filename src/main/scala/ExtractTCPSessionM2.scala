package cc.xmccc.sparkdemo

import java.net.InetAddress

import cc.xmccc.hbase.util.HBaseUtil
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.sql.SparkSession
import org.pcap4j.packet.factory.PacketFactories
import org.pcap4j.packet.namednumber.DataLinkType
import org.pcap4j.packet.{Packet, TcpPacket}

import scala.util.Try

/*
* extract sessions from tcp flow
*
* method 2
* */
object ExtractTCPSessionM2 {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.master", "hmaster.hbase:60000")
  val conn = ConnectionFactory.createConnection(conf)
  val admin = conn.getAdmin()

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("ExtractSession")
      .getOrCreate()

    val input_table = args(0)
    val save_table = args(1)

    val input_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte])](input_table)
      .select("di", "si", "dp", "sp", "pr", "t", "r", "fm" )
      .inColumnFamily("p")

    if (! admin.tableExists(TableName.valueOf(save_table)))
      createPresplitTable(save_table)

    val tcp_format_rdd = input_rdd.map{
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

    println("packets count: ")
    println(tcp_format_rdd.count())

    val grouped = tcp_format_rdd.groupBy(_._1)

    println("tuple5 counts: ")
    println(grouped.count())
/*    grouped.collect().foreach{
      case(_, it) =>
        println("==== split ====")
        it.toList.filter{
          case(_, _, _, flags, _, _, _, _, _) =>
            if(flags == 0x02 ||
              flags == 0x12 ||
              flags == 0x10) {
              true
            } else {
              false
            }
        }.sortBy(_._3).foreach{
          case(_, _, ts, flags, ackn, synn, _, _, _) =>
            println(ts.toString + " " + synn.toString + " " + ackn.toString + " " + flags.toString)
        }
    }*/


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

            val tcp_marks = (handshakes_ts_seq map (x => (0, x))) ++ (stop_seq map (x => (1, x))) //++ List((0, BigInt(0)), (1, BigInt(Array(0x00.toByte) ++ Array.fill(8)(0xff.toByte))))

            val tcp_mark_seq = tcp_marks sortBy (_._2)

            val tail = if (tcp_mark_seq.isEmpty) List.empty else tcp_mark_seq.tail
            //val tail = tcp_mark_seq.tail

            val tcp_range_seq = tcp_mark_seq zip tail filter {
              case((0, _), (1, _)) => true
              case((0, _), (0, _)) => true
              case _ => false
            } map {case((_, ts_b), (_, ts_s)) => (ts_b, ts_s)}

            val resultt = tcp_range_seq.map{
              case(ts_b, ts_s) =>
                val items_in_range = items.filter(item => item._3 >= ts_b && item._3 <= ts_s)
                val head_item_ts = items_in_range.head._3
                items_in_range.map(
                  item => (item._9, item._2 ++ ensureXByte(head_item_ts.toByteArray, 8), item._11, item._10)
                )
            }

            resultt.flatten

        }

    val saved_rdd = sessions_list_rdd.flatMap(x => x)

    val sessns_per_tuple5_rdd = sessions_list_rdd.map{
      list =>
        list.groupBy(x => BigInt(x._2)).toList.length
    }
/*    println("each tuple5's packet number")
    input_rdd.groupBy(x => BigInt(x._9)).map(_._2.toList.length).collect
        .foreach(x => print(x + " "))
    println()*/
    val packets_per_session =  sessions_list_rdd.flatMap{
      list =>
        val o = for {
          item <- list.lift(0)
          sid = item._2
          dip = InetAddress.getByAddress(sid.slice(0, 4))
          dport = BigInt(sid.slice(4, 6))
          sip = InetAddress.getByAddress(sid.slice(6, 10))
          sport = BigInt(sid.slice(10, 12))
          proto = sid.slice(12, 13)
          ts = BigInt(sid.slice(13, 21))
        } yield s"${sip}:${sport}-${dip}:${dport}_${ts}"

        list.groupBy(x => BigInt(x._2)).toList.map(xx => (o, xx._2.map(x => BigInt(x._3))))
    }.collect

    println("max packets per session: ")
    val pkt_num_per_sessions = packets_per_session.map(_._2.length)
    println(pkt_num_per_sessions.max)
    println(packets_per_session.maxBy(_._2.length))
    println("min packets per session: ")
    println(pkt_num_per_sessions.min)
    println(packets_per_session.minBy(_._2.length))

    println()
    println("average sessions per tuple5: ")
    println(sessns_per_tuple5_rdd.sum() / sessns_per_tuple5_rdd.count().toFloat)
    println("max sessions per tuple5: ")
    println(sessns_per_tuple5_rdd.max())

    println("sessions count: ")
    println(saved_rdd.groupBy(x => BigInt(x._2)).count())

    println("packets with session id count: ")
    println(saved_rdd.count())

    saved_rdd
      .toHBaseTable(save_table)
      .toColumns("sid", "t", "r")
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


