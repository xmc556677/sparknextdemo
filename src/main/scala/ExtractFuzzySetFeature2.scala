package cc.xmccc.sparkdemo

import java.security.MessageDigest

import cc.xmccc.hbase.util.HBaseUtil
import cc.xmccc.sparkdemo.schema.HBaseOpsUtil._
import cc.xmccc.sparkdemo.schema._
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import cc.xmccc.sparkdemo.schema.HBaseOpsUtil._
import cc.xmccc.sparkdemo.schema._
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy

object ExtractFuzzySetFeature2 {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.master", "hmaster.hbase:60000")
  val conn = ConnectionFactory.createConnection(conf)
  val admin = conn.getAdmin()

  def FuzzySetFeatureExtract(sessn_feature_rdd: RDD[NewSessionFeatureTable]): RDD[FuzzySetTable] = {
    val fuzzy_set_rdd = sessn_feature_rdd groupBy { x => BigInt(x.m) }

    val saved_rdd = fuzzy_set_rdd map {
      case (_, it) =>
        val list = it.toList
        val rowkey = MessageDigest.getInstance("MD5").digest(list(0).m)
        val m = list(0).m
        val features_list = list.map(sessn_row =>
          sessn_row.features_value
        ).toArray

        val features_names = list.lift(0).get.features_name

        val feature_sequence = features_list.foldLeft(Array.fill(features_names.length)(Array.empty[Double])) {
          case (result, item) =>
            val arr_i = result zip item
            arr_i.map { case (arr, i) => arr :+ i }
        }

        val fuzzyset_features = feature_sequence.map {
          feature =>
            val avg = feature.sum / feature.length
            val variance = feature.map(value => math.pow(value - avg, 2)).sum / feature.length
            val std_deviation = math.pow(variance, 0.5)
            val (left, right) = (avg - 0.6 * std_deviation, avg + 0.6 * std_deviation)
            val feature_filtered = feature.filter(value => value >= left && value <= right)
            val feature_filtered_avg = feature_filtered.sum / feature_filtered.length

            (feature_filtered.length.toDouble / feature.length.toDouble, feature_filtered_avg)
        }

        FuzzySetTable(
          rowkey, Some(m), None, Some(features_names), Some(fuzzyset_features), Some(""), Some("")
        )
    }

    saved_rdd
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("ExtractFuzzySetFeature2")
      .getOrCreate()

    val input_table = args(0)
    val output_table = args(1)
    val fzset_id = args(2)
    val fzset_id_b = (0 to fzset_id.length-1 by 2).map{
      i =>
        val hex = fzset_id.slice(i, i+2)
        Integer.parseInt(hex, 16).toByte
    }.toArray

    if (! admin.tableExists(TableName.valueOf(output_table)))
      createPresplitTable(output_table, "fzset")

    val input_rdd = sparkSession.sparkContext.hbaseTable[NewSessionFeatureTable](input_table)
      .select("sport", "dport", "direction", "m", "sid", "features_name", "features_value")
      .inColumnFamily("sessn")

    val filtered_rdd = input_rdd.filter{
      row =>
        BigInt(row.m) == BigInt(fzset_id_b)
    }

    val fuzzyset_feature_rdd = FuzzySetFeatureExtract(filtered_rdd)

    fuzzyset_feature_rdd.toHBaseTable(output_table)
      .toColumns("m", "id", "features_name", "features_value", "fkeywords", "bkeywords")
      .inColumnFamily("fzset")
      .save()

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
