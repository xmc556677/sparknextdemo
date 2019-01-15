package cc.xmccc.sparkdemo

import org.apache.spark.sql.SparkSession
import it.nerdammer.spark.hbase._
import cc.xmccc.sparkdemo.schema._
import cc.xmccc.sparkdemo.schema.HBaseOpsUtil._
import org.apache.hadoop.hbase.util.Bytes

object ExportModelFeatures {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("ExportMOdelFeatures")
      .getOrCreate()

    val model_table = args(0)

    val model_rdd = sparkSession.sparkContext.hbaseTable[ProtoModelTable](model_table)
      .select("id", "features_name", "features_value", "fkeywords", "bkeywords")
      .inColumnFamily("model")

    val local_model = model_rdd.collect()
    println(local_model.head.features_name.get.mkString(","))
    local_model.foreach{
      row =>
        val proto = Bytes.toString(row.id.get)
        println(proto ++ "," ++ row.features_value.get.map(x => s"${x._1},${x._2}").mkString(","))
    }

    sparkSession.close()
  }
}