package cc.xmccc.sparkdemo

import java.security.MessageDigest

import cc.xmccc.hbase.util.HBaseUtil
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

object StreamingToHBase {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.master", "hmaster.hbase:60000")
  val conn = ConnectionFactory.createConnection(conf)
  val admin = conn.getAdmin()

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("StreamingToHBase")
      .getOrCreate()


    val batch_duration = 10
    val kafka_topic = "pcap10"
    val kafka_servers = "192.168.0.145:9092,192.168.0.146:9092"
    val save_table = "xmc:raw_packets_2gb"

    if (! admin.tableExists(TableName.valueOf(save_table)))
      createPresplitTable(save_table)

    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(batch_duration))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafka_servers,
      "key.deserializer" -> classOf[ByteArrayDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "group.id" -> "KJSKK",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(kafka_topic)

    val stream = KafkaUtils.createDirectStream[Array[Byte], Array[Byte]](
      ssc,
      PreferConsistent,
      Subscribe[Array[Byte], Array[Byte]](topics, kafkaParams)
    )

    val rowkey_rawpackets_stream = stream.map{
      item =>
        val timestamp = item.key()
        val raw_packets = item.value()
        val timestamp_md5 = MessageDigest.getInstance("MD5").digest(timestamp)
        (timestamp_md5, raw_packets, timestamp)
    }

    rowkey_rawpackets_stream.foreachRDD{
      rdd =>
        rdd.toHBaseTable(save_table)
          .toColumns("r", "t")
          .inColumnFamily("packet")
          .save()
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def createPresplitTable(name: String): Unit = {
    val table_name_create_presplit = TableName.valueOf(name)
    val table_presplit = new HTableDescriptor(table_name_create_presplit)
    table_presplit.addFamily(new HColumnDescriptor("packet"))
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
