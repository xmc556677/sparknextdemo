package cc.xmccc.sparkdemo

import it.nerdammer.spark.hbase._
import org.apache.spark.sql.SparkSession
import cc.xmccc.sparkdemo.schema.HBaseOpsUtil._
import cc.xmccc.sparkdemo.schema.{SessionFeatureTable,FuzzySetAvgFeatureTable}
import cc.xmccc.sparkdemo.schema.{FuzzySetAvgFeatureTable, SessionFeatureTable}
import shapeless.{Generic, Poly1}

object ExtractFuzzySetFeature {

  object pairConsPoly1 extends Poly1 {
    implicit def listCase[A]: Case.Aux[(A, List[A]), List[A]] =
      at{case(a, b) => a :: b}
  }

  object stdVarCalcuPoly1 extends Poly1 {
    implicit def listCase[A](implicit num: Numeric[A]): Case.Aux[List[A], Double] =
      at{
        a =>
          val avg = num.toDouble(a.sum(num)) / a.length
          val variane = a.map(x => math.pow(num.toDouble(x) - avg, 2)).sum / a.length
          val std_deviation = math.pow(variane, 0.5)
          val (left, right) = (variane - 2 * std_deviation, variane + 2 * std_deviation)
          val a_filtered = a filter {
            x =>
              val n = num.toDouble(x)
              n > left && n < right
          }
          val avg_filtered = num.toDouble(a_filtered.sum(num)) / a_filtered.length

          avg_filtered
      }

    implicit val arraybyteCase: Case.Aux[List[Array[Byte]], Double] =
      at{a => 0}
  }

  object toListPoly1 extends Poly1 {
    implicit def numCase[A](implicit num: Numeric[A]): Case.Aux[A, List[A]] =
      at{a => List(a)}

    implicit val bytearrayCase: Case.Aux[Array[Byte], List[Array[Byte]]] =
      at{a => List(a)}
  }
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Extract")
      .getOrCreate()

    val input_table = args(0)
    val save_table = args(1)

    val input_rdd = sparkSession.sparkContext.hbaseTable[SessionFeatureTable](input_table)
      .select("avg_pkt_len", "min_pkt_len", "max_pkt_len", "var_pkt_len",
        "avg_ts_IAT", "min_ts_IAT", "max_ts_IAT", "var_ts_IAT",
        "avg_pld_len", "min_pld_len", "max_pld_len", "var_pld_len",
        "total_bytes", "sessn_dur", "pkts_cnt", "psh_cnt", "sport", "dport",
        "direction", "m",
        "sc_avg_pkt_len", "sc_min_pkt_len", "sc_max_pkt_len", "sc_var_pkt_len",
        "sc_avg_ts_IAT", "sc_min_ts_IAT", "sc_max_ts_IAT", "sc_var_ts_IAT",
        "sc_avg_pld_len", "sc_min_pld_len", "sc_max_pld_len", "sc_var_pld_len",
        "sc_total_bytes", "sc_pkt_cnt",
        "cs_avg_pkt_len", "cs_min_pkt_len", "cs_max_pkt_len", "cs_var_pkt_len",
        "cs_avg_ts_IAT", "cs_min_ts_IAT", "cs_max_ts_IAT", "cs_var_ts_IAT",
        "cs_avg_pld_len", "cs_min_pld_len", "cs_max_pld_len", "cs_var_pld_len",
        "cs_total_bytes", "cs_pkt_cnt")
      .inColumnFamily("p")

    val fuzzy_set_rdd = input_rdd groupBy { x => BigInt(x.m) }


    val saved_rdd = fuzzy_set_rdd map {
      case (_, it) =>
        val list = it.toList
        val rowkey = list(0).m
        val repr_list = list map { x => Generic[SessionFeatureTable].to(x).filterNot[Array[Byte]] }
        val repr_head :: repr_tail = repr_list
        val repr_list_head = repr_head map toListPoly1
        val list_hlist = repr_tail.foldLeft(repr_list_head) {
          case (b, a) =>
            val aAb = a zip b
            aAb map pairConsPoly1
        }

        val avg_hlist = list_hlist.map(stdVarCalcuPoly1)

        Generic[FuzzySetAvgFeatureTable].from(rowkey :: avg_hlist)
    }

    saved_rdd
      .toHBaseTable(save_table)
      .toColumns("avg_pkt_len", "min_pkt_len", "max_pkt_len", "var_pkt_len",
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
      .save()

    sparkSession.close()
  }
}
