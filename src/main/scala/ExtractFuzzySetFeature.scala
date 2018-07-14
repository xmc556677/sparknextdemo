package cc.xmccc.sparkdemo

import it.nerdammer.spark.hbase._
import org.apache.spark.sql.SparkSession

object ExtractFuzzySetFeature {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Extract")
      .getOrCreate()

    val input_table = args(0)
    val save_table = args(1)

    val input_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte])](input_table)
      .select("di", "si", "dp", "sp", "pr", "t", "r", "m" )
      .inColumnFamily("p")
  }
}
