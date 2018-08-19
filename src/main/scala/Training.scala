package cc.xmccc.sparkdemo

import java.security.MessageDigest

import cc.xmccc.hbase.util.HBaseUtil
import cc.xmccc.sparkdemo.schema.HBaseOpsUtil._
import cc.xmccc.sparkdemo.schema.{FuzzySetAvgFeatureTable, SessionFeatureToExtract}
import org.apache.spark.sql.SparkSession
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import cc.xmccc.sparkdemo.Utils.repr_string
import shapeless.labelled.FieldType
import shapeless.{Generic, LabelledGeneric, Poly1, Witness}

object Training {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.master", "hmaster.hbase:60000")
  val conn = ConnectionFactory.createConnection(conf)
  val admin = conn.getAdmin()
  val sparkSession = SparkSession.builder()
    .appName("Training")
    .getOrCreate()

  def slicePacket(str: Array[Byte], n: Int): List[Array[Byte]] =
    (0 to str.length - n).map(i => str.slice(i, i+n)).toList

  def stringToByteArray(str: String): Array[Byte] = {
    str.toCharArray.map(_.toByte)
  }

  object fieldTypePoly1 extends Poly1 {
    implicit def doubleCase[K](implicit witness: Witness.Aux[K]): Case.Aux[FieldType[K, (Double, Double)], (String, (Double, Double))] =
      at(kv => (witness.value.toString, kv))
  }

  object pairConsPoly1 extends Poly1 {
    implicit def listCase[A]: Case.Aux[(A, List[A]), List[A]] =
      at{case(a, b) => a :: b}
  }

  object percentAvgCalcuPoly1 extends Poly1 {
    implicit def listCase[A](implicit num: Numeric[A]): Case.Aux[List[A], (Double, Double)] =
      at{
        a =>
          val avg = num.toDouble(a.sum) / a.length
          val variane = a.map(x => math.pow(num.toDouble(x) - avg, 2)).sum / a.length
          val std_deviation = math.pow(variane, 0.5)
          val (left, right) = (avg - 0.5 * std_deviation, avg + 0.5 * std_deviation)
          val a_filtered = a filter {
            x =>
              val n = num.toDouble(x)
              n >= left && n <= right
          }
          val avg_filtered = num.toDouble(a_filtered.sum) / a_filtered.length

          (a_filtered.length.toDouble / a.length.toDouble, avg_filtered)
      }

    implicit val arraybyteCase: Case.Aux[List[Array[Byte]], (Double, Double)] =
      at{a => (0, 0)}
  }

  object toListPoly1 extends Poly1 {
    implicit def numCase[A](implicit num: Numeric[A]): Case.Aux[A, List[A]] =
      at{a => List(a)}

    implicit val bytearrayCase: Case.Aux[Array[Byte], List[Array[Byte]]] =
      at{a => List(a)}
  }

  def FuzzySetFeatureExtract(sessn_feature_rdd: RDD[SessionFeatureToExtract]): RDD[FuzzySetAvgFeatureTable] = {
    val fuzzy_set_rdd = sessn_feature_rdd groupBy { x => BigInt(x.m) }

    val saved_rdd = fuzzy_set_rdd map {
      case (_, it) =>
        val list = it.toList
        val rowkey = MessageDigest.getInstance("MD5").digest(list(0).m)
        val fm = list(0).m
        val repr_list = list map { x => Generic[SessionFeatureToExtract].to(x)}
        val repr_head :: repr_tail = repr_list
        val repr_list_head = repr_head map toListPoly1
        val list_hlist = repr_tail.foldLeft(repr_list_head) {
          case (b, a) =>
            val aAb = a zip b
            aAb map pairConsPoly1
        }

        val avg_hlist = list_hlist.map(percentAvgCalcuPoly1)

        Generic[FuzzySetAvgFeatureTable].from(rowkey :: fm :: (avg_hlist.tail.tail))
    }

    saved_rdd
  }

  def FuzzySetKeywordsExtract(fuzzysetmark_sid_direction_pld_rdd: RDD[(Array[Byte], Option[Array[Byte]], Option[Array[Byte]], Option[Array[Byte]], Option[Array[Byte]])]): RDD[(Array[Byte], String)] = {
    val sessn_pkts_rdd = fuzzysetmark_sid_direction_pld_rdd.filter{
      case(_, Some(_), Some(_), Some(_), _) => true
      case _ => false
    }

    val fuzzyset_direction_sessn_rdd = sessn_pkts_rdd.groupBy{
      case(_, Some(fuzzyset_mark), Some(session_mark), Some(direction), _) =>
        (BigInt(fuzzyset_mark), BigInt(session_mark), BigInt(direction))
    }

    val fuzzyset_directions = sessn_pkts_rdd.groupBy{
      case(_, Some(fuzzyset_mark), Some(session_mark), Some(direction), _) =>
        (BigInt(fuzzyset_mark), BigInt(direction))
    }.map{case((fuzzy_mark, direction), _) => (fuzzy_mark, direction)}.collect()

    val sessn_rdd = fuzzyset_direction_sessn_rdd.map{
      case((fuzzyset_mark, _, direction), it) =>
        val ls = it.toList
        val sessn_plds = ls.map(_._5.get)
        val sliced_plds = sessn_plds.map{
          pld =>
            List.range(0, pld.length - 3).map(x => String.valueOf(pld.slice(x, x+3).map(x => Character.toChars(x & 0x00ff)).flatten)) toArray
        } toArray
        val words_propotion = sliced_plds
          .flatten
          .groupBy(x => x)
          .map{case(k, its) => (k, its.length.toDouble / sessn_plds.length.toDouble)}
          .filter(x => x._2 >= 0.5 && x._2 <= 1.0)
          .toArray

        ((fuzzyset_mark, direction), words_propotion)
    }

    val models = fuzzyset_directions.map{
      mark =>
        val fuzzyset_direction_rdd = sessn_rdd.filter{
          case(m, _) =>
            m == mark
        }.map(_._2.map(_._1)).sample(false, 0.05)

        println("[\n")
        fuzzyset_direction_rdd.collect.foreach{
          x =>
            println("\t" + x.map(x => repr_string(x)).mkString("[", ",", "]"))
        }
        println("]\n")

        val fpg = new FPGrowth()
          .setMinSupport(0.2)
          .setNumPartitions(10)

        val rowkey = MessageDigest.getInstance("MD5").digest(mark._1.toByteArray)

        (rowkey, fpg.run(fuzzyset_direction_rdd))
    }

    models.foreach{
      case (_, model) =>
        println(s"model: $model")

        model.freqItemsets.take(100).foreach{
          itemset =>
            println(itemset.items.map(x => repr_string(x)).mkString("[", ",", "]") + ","+ itemset.freq)
        }
    }

    val fuzzyset_keywords = models.map{
      case(rowkey, model) =>
        val freq_words = model.freqItemsets.take(10).toList
        val keywords = freq_words.map{
          freq_set =>
            freq_set.items.toList
        }.flatten.toSet.toList

        (rowkey,
          keywords
            .map(x => java.util.Base64.getEncoder.encodeToString(stringToByteArray(x)))
            .mkString(",")
        )
    }

    val save_rdd = sparkSession.sparkContext.parallelize(fuzzyset_keywords)

    save_rdd
  }

  def FuzzySetClustering(fuzzyset_feature_rdd: RDD[FuzzySetAvgFeatureTable]) = {
    val kv_input_rdd: RDD[(Array[Byte], List[(String, (Double, Double))])] = fuzzyset_feature_rdd.map{
      avg =>
        val gen =  LabelledGeneric[FuzzySetAvgFeatureTable].to(avg)
        val kv_fuzzy_set_feature = gen.tail.tail
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

    val clustered_fuzzyset = sets.zipWithIndex.map{
      case(items, cluster_id) =>
        items.map{
          case(rowkey, _) =>
            (rowkey, cluster_id)
        }
    }.flatten

    val saved_rdd = sparkSession.sparkContext.parallelize(clustered_fuzzyset)

    saved_rdd
  }


  def main(args: Array[String]): Unit = {
    val input_table = args(0)
    val input_table2 = args(1)
    val save_table = args(2)
    val save_table2 = args(3)
    val fuzzyset_mark = args(4)

    val input_rdd = sparkSession.sparkContext.hbaseTable[SessionFeatureToExtract](input_table)
      .select("m", "avg_pkt_len", "min_pkt_len", "max_pkt_len", "var_pkt_len",
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
      .inColumnFamily("sessn")

    val pld_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Option[Array[Byte]], Option[Array[Byte]], Option[Array[Byte]], Option[Array[Byte]])](input_table2)
      .select("m", "sid", "direction", "payload")
      .inColumnFamily("p")

    val training_rdd = input_rdd.filter{
      features =>
        BigInt(features.m) == BigInt(fuzzyset_mark)
    }

    val keywords_training_rdd = pld_rdd.filter{
      item =>
        BigInt(item._1) == BigInt(fuzzyset_mark)
    }

    if (! admin.tableExists(TableName.valueOf(save_table)))
      createPresplitTable(save_table, "p")
    if (! admin.tableExists(TableName.valueOf(save_table2)))
      createPresplitTable(save_table2, "sessn")

    val fuzzyset_feature_rdd = FuzzySetFeatureExtract(training_rdd)

    val fuzzyset_keywords_rdd = FuzzySetKeywordsExtract(keywords_training_rdd)

    fuzzyset_feature_rdd.toHBaseTable(save_table)
      .toColumns("m", "avg_pkt_len", "min_pkt_len", "max_pkt_len", "var_pkt_len",
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
      .save

    fuzzyset_keywords_rdd.toHBaseTable(save_table)
      .toColumns("keywords")
      .inColumnFamily("avg")
      .save()

    val cluster_data_rdd = sparkSession.sparkContext.hbaseTable[FuzzySetAvgFeatureTable](input_table)
      .select("m", "avg_pkt_len", "min_pkt_len", "max_pkt_len", "var_pkt_len",
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

    val cluster_rdd = FuzzySetClustering(cluster_data_rdd)
    cluster_rdd.toHBaseTable(save_table)
      .toColumns("id")
      .inColumnFamily("avg")
      .save()

    sparkSession.close()
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
