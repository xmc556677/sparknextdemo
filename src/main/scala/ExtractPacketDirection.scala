package cc.xmccc.sparkdemo

import cc.xmccc.hbase.util.HBaseUtil
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import org.apache.spark.sql.SparkSession

import it.nerdammer.spark.hbase._

object ExtractPacketDirection {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.master", "hmaster.hbase:60000")
  val conn = ConnectionFactory.createConnection(conf)
  val admin = conn.getAdmin()

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("ExtractPacketDirection")
      .getOrCreate()

    val input_table = args(0)
    val save_table = args(1)

    val input_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Option[Array[Byte]])](input_table)
      .select("si", "sp", "di", "dp", "pr", "t", "sid")
      .inColumnFamily("p")

    val saved_rdd = input_rdd.filter(_._8 != None).groupBy(x => BigInt(x._8.get))
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

    saved_rdd.toHBaseTable(save_table)
      .toColumns("direction", "m")
      .inColumnFamily("p")
      .save()

    sparkSession.close()
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
