package cc.xmccc.sparkdemo

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
      .select("di", "si", "dp", "sp", "pr", "t", "r", "m" )
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
            }

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
                  case (0, first) :: (1, second) :: _ if (ts >= first && ts <= second) =>
                    ((rowkey, Some(mark_b ++ ensureXByte(first.toByteArray, 8)), ts_b, rawpacket) :: result, ts_seq)
                  case (0, first) :: (1, second) :: (0, third) :: _  if (ts >= third) =>
                    ((rowkey, Some(mark_b ++ ensureXByte(third.toByteArray, 8)), ts_b, rawpacket) :: result, ts_seq.drop(2))
                  case (0, first) :: (0, second) :: _ if (ts >= first && ts <= second) =>
                    ((rowkey, Some(mark_b ++ ensureXByte(first.toByteArray, 8)), ts_b, rawpacket) :: result, ts_seq)
                  case (0, first) :: (0, second) :: _ =>
                    ((rowkey, Some(mark_b ++ ensureXByte(second.toByteArray, 8)), ts_b, rawpacket) :: result, ts_seq.drop(1))
                  case (0, first) :: Nil if(ts >= first) =>
                    ((rowkey, Some(mark_b ++ ensureXByte(first.toByteArray, 8)), ts_b, rawpacket) :: result, ts_seq)
                  case (_, first) :: _ if (ts <= first) =>
                    ((rowkey, None, ts_b, rawpacket) :: result, ts_seq)
                  case _ =>
                    ((rowkey, None, ts_b, rawpacket) :: result, ts_seq.drop(1))
                }
            }

            result._1.filter(_._2 != None).map(x => (x._1, x._2.get, x._3, x._4))
        }

    val saved_rdd = sessions_list_rdd.flatMap(x => x)

    val sessns_per_tuple5_rdd = sessions_list_rdd.map{
      list =>
        list.groupBy(x => BigInt(x._2)).toList.length
    }
    println("each tuple5's packet number")
    input_rdd.groupBy(x => BigInt(x._9)).map(_._2.toList.length).collect
        .foreach(x => print(x + " "))
    println()
    println("each session's packet:")
    sessions_list_rdd.flatMap{
      list =>
        list.groupBy(x => BigInt(x._2)).toList.map(_._2.map(x => BigInt(x._3)))
    }.collect.foreach{
      x =>
        println(x.length)
        println(x.sorted)
    }
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


