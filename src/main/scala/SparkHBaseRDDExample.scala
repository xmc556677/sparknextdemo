package cc.xmccc.sparkdemo

import cc.xmccc.hbase.util.HBaseUtil
import org.apache.spark.sql.SparkSession
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import org.apache.hadoop.hbase.util.Bytes

object SparkHBaseRDDExample {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.master", "hmaster.hbase:60000")
  val conn = ConnectionFactory.createConnection(conf)
  val admin = conn.getAdmin()

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("SparkHBaseRDDExample")
      .getOrCreate()

    /*
    * read data from hbase in column "p:r" table "xmc:rawpackets"
    * */
    val orig_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Array[Byte])]("xmc:rawpackets")
      .select("r")
      .inColumnFamily("p")

    //take 2 result to print
    orig_rdd.take(2).foreach{
      case (r, x) =>
        println("rowkey:")
        println(r.map(x => (x & 0x00ff).toHexString).mkString(" "))
        println("raw packet:")
        println(x.map(x => (x & 0x00ff).toHexString).mkString(" "))
    }

    if (! admin.tableExists(TableName.valueOf("dummytestt:t")))
      createPresplitTable("dummytestt:t")

    /*
    * take elements from original rdd then convert to (rowkey, hex string)
    * pair to save to the HBase
    * */

    val save_rdd = orig_rdd.map{
      case (r, x) =>
        (r, x.map(x => (x & 0x00ff).toHexString).mkString(" "))
    }

    save_rdd.toHBaseTable("dummytestt:t")
      .toColumns("t")
      .inColumnFamily("test")
      .save()

    sparkSession.close()
    admin.close()
    conn.close()
  }

  def createPresplitTable(name: String): Unit = {
    val table_name_create_presplit = TableName.valueOf(name)
    val table_presplit = new HTableDescriptor(table_name_create_presplit)
    table_presplit.addFamily(new HColumnDescriptor("test"))
    //config table to presplit rowkey
    table_presplit.setValue(HTableDescriptor.SPLIT_POLICY, classOf[KeyPrefixRegionSplitPolicy].getName)
    //set the prefix length of rowkey for presplit
    table_presplit.setValue(KeyPrefixRegionSplitPolicy.PREFIX_LENGTH_KEY, "4")

    admin.createTable(
      table_presplit,
      HBaseUtil.getHexSplits(
        Bytes.toBytes(0.toInt),
        Bytes.toBytes(120.toInt),
        15
      )
    )
  }
}