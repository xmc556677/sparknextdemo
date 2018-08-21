package cc.xmccc.sparkdemo

import org.apache.spark.sql.SparkSession
import cc.xmccc.sparkdemo.schema.HBaseOpsUtil._
import cc.xmccc.sparkdemo.schema.{NewSessionFeatureTable, ProtoModelTable}
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.util.Bytes

object Recognize {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Recognize")
      .getOrCreate()

    val input_table = args(0)
    val input_table2 = args(1)
    val session_id = args(2)
    val session_id_b = session_id
      .split("\\\\x")
      .tail
      .map(hexstr => Integer.parseInt(hexstr, 16).toByte)

    val session_table_rdd = sparkSession.sparkContext.hbaseTable[NewSessionFeatureTable](input_table)
      .select("sport", "dport", "direction", "m", "sid", "features_name", "features_value")
      .inColumnFamily("sessn")

    val model_rdd = sparkSession.sparkContext.hbaseTable[ProtoModelTable](input_table2)

    val session_row = session_table_rdd
      .filter(row => BigInt(row.rowkey) == BigInt(session_id_b))
      .collect
      .lift(0)

    val session_row_featuremap = session_row.map{
      row =>
        val features_map = (row.features_name zip row.features_value).toMap
        (row, features_map)
    }.get

    val broad_feature_map = sparkSession.sparkContext.broadcast(session_row_featuremap)

    val recognize_result = model_rdd.map{
      model_row =>
        val name_features = model_row.features_name.get zip model_row.features_value.get
        val top10_features = name_features.sortBy(_._2._1).reverse.take(10)
        val proportion = top10_features.map{
          case(k, (_, v)) =>
            val value = v / broad_feature_map.value._2(k)
            if(value.isInfinite)
              0
            else if (value.isNaN)
              1
            else
              value
        }.filter(x => x >= 0.4 && x <= 2.08)

        if(proportion.length >= 8)
          Some(Bytes.toString(model_row.rowkey))
        else
          None
    }

    val result = recognize_result
      .filter(result => result !=  None)
      .collect
      .lift(0)

    println(s"Session id: ${session_id}")
    result match {
      case Some(proto) => println(s"Corespond Protocol: ${proto}")
      case None => println(s"Corespond Protocol: None")
    }
  }
}
