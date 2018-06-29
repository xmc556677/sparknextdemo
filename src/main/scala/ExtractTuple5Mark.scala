package cc.xmccc.sparkdemo

import cc.xmccc.hbase.util.HBaseUtil
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.sql.SparkSession

object ExtractTuple5Mark {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.master", "hmaster.hbase:60000")
  val conn = ConnectionFactory.createConnection(conf)
  val admin = conn.getAdmin()

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("ExtractSession")
      .getOrCreate()

    val input_table = "xmc:tuple5_2gb"
    val save_table = "xmc:tuple5_2gb"

    val input_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte])](input_table)
      .select("di", "si", "dp", "sp", "pr", "t", "r" )
      .inColumnFamily("p")

    if (! admin.tableExists(TableName.valueOf(save_table)))
      createPresplitTable(save_table)

    val save_rdd = input_rdd.map{
      case(rowkey, dip_b, sip_b, dport_b, sport_b, proto_b, ts_b, rawpacket) =>
        val destination_b = dip_b ++ dport_b
        val source_b = sip_b ++ sport_b
        val first :: second :: Nil = List(destination_b, source_b).sortBy(_.mkString(""))
        val tuple5_b = first ++ second ++ proto_b

        (rowkey, tuple5_b)
    }

    save_rdd
      .toHBaseTable(save_table)
      .toColumns("mark")
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


