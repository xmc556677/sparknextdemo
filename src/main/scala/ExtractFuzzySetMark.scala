package cc.xmccc.sparkdemo

import cc.xmccc.sparkdemo.schema.FuzzySetAvgFeatureTable
import org.apache.spark.sql.SparkSession
import cc.xmccc.sparkdemo.schema.HBaseOpsUtil._
import it.nerdammer.spark.hbase._
import org.apache.spark.rdd.RDD
import shapeless.labelled.FieldType
import shapeless.{LabelledGeneric, Poly1, Witness}
import cc.xmccc.sparkdemo.Utils.repr_string

import scala.annotation.tailrec

object ExtractFuzzySetMark {

  object fieldTypePoly1 extends Poly1 {
    implicit def doubleCase[K](implicit witness: Witness.Aux[K]): Case.Aux[FieldType[K, (Double, Double)], (String, (Double, Double))] =
      at(kv => (witness.value.toString, kv))
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("ExtractFuzzySetMark")
      .getOrCreate()

    val input_table = args(0)
    val save_table = args(1)

    val input_rdd = sparkSession.sparkContext.hbaseTable[FuzzySetAvgFeatureTable](input_table)
      .select("avg_pkt_len", "min_pkt_len", "max_pkt_len", "var_pkt_len",
        "avg_ts_IAT", "min_ts_IAT", "max_ts_IAT", "var_ts_IAT",
        "avg_pld_len", "min_pld_len", "max_pld_len", "var_pld_len",
        "total_bytes", "sessn_dur", "pkts_cnt", "psh_cnt",
        "sc_avg_pkt_len", "sc_min_pkt_len", "sc_max_pkt_len", "sc_var_pkt_len",
        "sc_avg_ts_IAT", "sc_min_ts_IAT", "sc_max_ts_IAT", "sc_var_ts_IAT",
        "sc_avg_pld_len", "sc_min_pld_len", "sc_max_pld_len", "sc_var_pld_len",
        "sc_total_bytes", "sc_pkt_cnt",
        "cs_avg_pkt_len", "cs_min_pkt_len", "cs_max_pkt_len", "cs_var_pkt_len",
        "cs_avg_ts_IAT", "cs_min_ts_IAT", "cs_max_ts_IAT", "cs_var_ts_IAT",
        "cs_avg_pld_len", "cs_min_pld_len", "cs_max_pld_len", "cs_var_pld_len",
        "cs_total_bytes", "cs_pkt_cnt")
      .inColumnFamily("avg")

    val kv_input_rdd: RDD[(Array[Byte], List[(String, (Double, Double))])] = input_rdd.map{
      avg =>
        val gen =  LabelledGeneric[FuzzySetAvgFeatureTable].to(avg)
        val kv_fuzzy_set_feature = gen.tail
        val rowkey = gen.head
        (rowkey, kv_fuzzy_set_feature.map(fieldTypePoly1).toList[(String, (Double, Double))])
    }

    val sets = kv_input_rdd.collect.foldLeft(
      List.empty[List[(Array[Byte], List[(String, (Double, Double))])]]
    ){
      case (Nil, (rowkey, cur)) =>
        List((rowkey, cur)) :: Nil
      case (collect, (rowkey, cur)) =>
        val top_10_feature = cur.sortBy(_._2._1).reverse.take(10)
        println("top 10 feature:")
        println(top_10_feature)
        println("current sets:")
        println("[")
        collect.map(x => x.map(_._2)).foreach{
          x =>
            println("\t[")
            println("\t\t" + x.mkString("[", ",\n", "]"))
            println("\t]")
        }
        val propotions_each_set = (0 to collect.length - 1).map{
          i =>
            val (_, features) = collect(i)(0)
            val features_map = features.toMap
            top_10_feature.map{
              case(k, v) =>
                val value = features_map(k)._2 / v._2
                if(value.isInfinite)
                  0
                else if (value.isNaN)
                  1
                else
                  value
            }
        }
        propotions_each_set.foreach{
          x =>
            println("\t[")
            println("\t\t" + x.mkString("[", ",\n\t\t\t", "]"))
            println("\t]")
        }
        val result = (0 to collect.length - 1).toStream.map{
          i =>
            val (_, features) = collect(i)(0)
            val features_map = features.toMap
            val propotions = top_10_feature
              .map{
                case(k, v) =>
                  val value = features_map(k)._2 / v._2
                  if(value.isInfinite)
                    0
                  else if (value.isNaN)
                    1
                  else
                    value
              }
              .filter(x => x >= 0.4 && x <= 2.08)
            if(propotions.length >= 8) {
              val new_set = (rowkey, cur) :: collect(i)
              new_set :: collect.drop(i + 1) ++ collect.take(i)
            } else {
              Nil
            }
        }.filter(l => l != Nil)
            .take(1)
            .lift(0)

        result match {
          case None =>
            List((rowkey, cur)) :: collect
          case Some(r) =>
            r
        }
    }


    sets.zipWithIndex.foreach{
      case(datas, id) =>
        val datas_rowkey = datas.map(x => repr_string(String.valueOf(x._1.map(x => Character.toChars(x & 0x00ff)).flatten))).toList
        println(s"${id}:")
        println(datas_rowkey)
    }

    val clustered_fuzzyset = sets.zipWithIndex.map{
      case(items, cluster_id) =>
        items.map{
          case(rowkey, _) =>
            (rowkey, cluster_id)
        }
    }.flatten

    val saved_rdd = sparkSession.sparkContext.parallelize(clustered_fuzzyset)

    saved_rdd.toHBaseTable(save_table)
      .toColumns("id")
      .inColumnFamily("avg")
      .save()

    sparkSession.close()
  }
}
