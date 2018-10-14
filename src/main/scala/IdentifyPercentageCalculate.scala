package cc.xmccc.sparkdemo

import org.apache.spark.sql.SparkSession
import it.nerdammer.spark.hbase._
import cc.xmccc.sparkdemo.schema._
import cc.xmccc.sparkdemo.schema.HBaseOpsUtil._
import org.apache.hadoop.hbase.util.Bytes

object IdentifyPercentageCalculate {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("IdentifyPercentageCalculate")
      .getOrCreate()

    val model_table = args(0)
    val sessn_table = args(1)
    val dest_proto = args(2)

    val model_rdd = sparkSession.sparkContext.hbaseTable[ProtoModelTable](model_table)
      .select("id", "features_name", "features_value", "keywords")
      .inColumnFamily("model")

    val sessn_rdd = sparkSession.sparkContext.hbaseTable[NewSessionFeatureTable](sessn_table)
      .select("sport", "dport", "direction", "m", "sid", "features_name", "features_value")
      .inColumnFamily("sessn")

    val sessions_featuremap_rdd = sessn_rdd.map{
      row =>
        val features_map = (row.features_name zip row.features_value).toMap
        (row, features_map)
    }

    val models = model_rdd.collect().map{
      row =>
        val features_map = (row.features_name.get zip row.features_value.get).toMap
        (row, features_map)
    }

    val broad_model = sparkSession.sparkContext.broadcast(models)

    val identify_result = sessions_featuremap_rdd.map{
      case (_, feature_map) =>
        val identify_result =
          broad_model.value.toStream.map{
            case(table, model_feature_map) =>
              val top10_features = model_feature_map.toList.sortBy(_._2._1).reverse.take(10)
              val proportion = top10_features.map{
                case(k, (_, v)) =>
                  val value = v / feature_map.get(k).get
                  if(value.isInfinite)
                    0
                  else if (value.isNaN)
                    1
                  else
                    value
              }.filter(x => x >= 0.4 && x <= 2.08)

              if(proportion.length >= 8)
                Some(Bytes.toString(table.rowkey))
              else
                None
          }

        val result = identify_result.filter(_ != None).map(_.get).take(1).lift(0)

        result
    }

    val postive_count = identify_result.filter{
      _ match {
        case Some(n) => n == dest_proto
        case _ => false
      }
    }.count

    println(s"Correct Identify Count: ${postive_count} / ${identify_result.count}")
  }
}