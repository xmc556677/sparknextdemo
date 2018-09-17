package cc.xmccc.sparkdemo

import cc.xmccc.sparkdemo.schema.HBaseOpsUtil._
import cc.xmccc.sparkdemo.schema.{FuzzySetTable, NewSessionFeatureTable}
import org.apache.spark.sql.SparkSession
import it.nerdammer.spark.hbase._
import org.apache.spark.sql.functions.{col, udf}

object FuzzySetQuery{

  def convert_binary_tostring(bin: Array[Byte]): String = {
    bin.map(x => "%02x".format(x & 0x00ff)).mkString("")
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Query")
      .getOrCreate()

    val input_table = args(0)

    val sessions_rdd = sparkSession.sparkContext.hbaseTable[NewSessionFeatureTable](input_table)
      .select("sport", "dport", "direction", "m", "sid", "features_name", "features_value")
      .inColumnFamily("sessn")

    import sparkSession.implicits._

    val convert_binary_tostirng_udf = udf(convert_binary_tostring(_:Array[Byte]): String)

    val fuzzyset_df = sessions_rdd.map(x => x).toDS

    fuzzyset_df.select(convert_binary_tostirng_udf(col("m"))).distinct().show()

    sparkSession.close()
  }
}