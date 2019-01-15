package cc.xmccc.sparkdemo

import java.security.MessageDigest

import cc.xmccc.hbase.util.HBaseUtil
import cc.xmccc.sparkdemo.schema.HBaseOpsUtil._
import cc.xmccc.sparkdemo.schema._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import cc.xmccc.sparkdemo.Utils.repr_string
import org.apache.hadoop.hbase.util.Bytes
import shapeless.labelled.FieldType
import shapeless.{Generic, LabelledGeneric, Poly1, Witness}
import org.apache.spark.sql.functions.col

import scala.util.Try

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

        val feature_sequence = features_list.foldLeft(Array.fill(features_names.length)(Array.empty[Double])){
          case(result, item) =>
            val arr_i = result zip item
            arr_i.map{case(arr, i) => arr :+ i}
        }

        val fuzzyset_features = feature_sequence.map{
          feature =>
            val avg = feature.sum / feature.length
            val variance = feature.map(value => math.pow(value - avg, 2)).sum / feature.length
            val std_deviation = math.pow(variance, 0.5)
            val (left, right) = (avg - 0.5 * std_deviation, avg + 0.5 * std_deviation)
            val feature_filtered = feature.filter(value => value >= left && value <= right)
            val feature_filtered_avg = feature_filtered.sum / feature_filtered.length

            (feature_filtered.length.toDouble / feature.length.toDouble, feature_filtered_avg)
        }

        FuzzySetTable(
          rowkey, Some(m), None, Some(features_names), Some(fuzzyset_features), None, None
        )
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

        val fpg = new FPGrowth()
          .setMinSupport(0.2)
          .setNumPartitions(10)

        val rowkey = MessageDigest.getInstance("MD5").digest(mark._1.toByteArray)

        (rowkey, fpg.run(fuzzyset_direction_rdd))
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

  def ProtoModelExtract(fuzzyset_feature_rdd: RDD[FuzzySetTable],
                        model_table_rdd: RDD[ProtoModelTable],
                        protocol_name: String): (RDD[(Array[Byte], Array[Byte])], RDD[ProtoModelTable]) = {
    val fuzzyset = fuzzyset_feature_rdd.collect
    val model_table = model_table_rdd.collect

    val fuzzyset_map = fuzzyset.map{
      table =>
        val name_features = table.features_name.get zip table.features_value.get
        (table, name_features.toMap)
    }

    val fuzzyset_models = fuzzyset_map.map{
      case (fuzzyset_row, fzset_map) =>
        val clustering = (0 to model_table.length - 1).toStream.map{
          i =>
            val model_row = model_table(i)

            val top10_features =
              (model_row.features_name.get zip model_row.features_value.get)
                .sortBy(_._2._1).reverse.take(10)

            val propotions = top10_features.map{
              case(k, (_, v)) =>
                val value = fzset_map(k)._2 / v
                if(value.isInfinite)
                  0
                else if (value.isNaN)
                  1
                else
                  value
            }
            val filtered_propotion = propotions.filter(x => x >= 0.4 && x <= 2.09)

            println(propotions.toList)

            if(filtered_propotion.length >= 8)
              Some((fuzzyset_row, model_row))
            else
              None
        }.filter(x => x != None).take(1).map(_.get).lift(0)

        clustering match {
          case None =>
            (fuzzyset_row, None)
          case Some((fuzzyset_row, model_row)) =>
            (fuzzyset_row, Some(model_row))
        }
    }

    val new_fuzzyset_model = fuzzyset_models.filter(_._2 == None)
    val old_fuzzyset_model = fuzzyset_models.filter(_._2 != None)

    val new_add_fuzzyset_model =
      ((model_table.length to model_table.length + new_fuzzyset_model.length) zip new_fuzzyset_model)
          .map{
            case(i, (fuzzyset_row, _)) =>
              //val model_id_b = Bytes.toBytes("WZ%03d".format(i))
              val model_id_b = Bytes.toBytes(protocol_name)
              (fuzzyset_row,
                Some(ProtoModelTable(
                  model_id_b, Some(model_id_b), fuzzyset_row.features_name, fuzzyset_row.features_value,
                  fuzzyset_row.fkeywords, fuzzyset_row.bkeywords
                ))
              )
          }

    val fuzzyset_models_new = new_add_fuzzyset_model ++ old_fuzzyset_model

    val seted_id_in_fuzzyset = fuzzyset_models_new.map{
      case(fuzzyset_row, model_row) =>
        ((fuzzyset_row.rowkey, model_row.get.id.get), model_row)
    }

    val fuzzyset_rowkey_id = seted_id_in_fuzzyset.map(_._1)
    val model = seted_id_in_fuzzyset.map(_._2.get)

    (sparkSession.sparkContext.parallelize(fuzzyset_rowkey_id),
    sparkSession.sparkContext.parallelize(model))
  }

  def main(args: Array[String]): Unit = {
    val input_table = args(0)
    val input_table2 = args(1)
    val save_table = args(2)
    val save_table2 = args(3)
    val fuzzyset_mark = args(4)
    val protocol_name = args(5)

    val fuzzyset_mark_b = (0 to fuzzyset_mark.length-1 by 2).map{
      i =>
        val hex = fuzzyset_mark.slice(i, i+2)
        Integer.parseInt(hex, 16).toByte
    }.toArray

    val input_rdd = this.sparkSession.sparkContext.hbaseTable[NewSessionFeatureTable](input_table2)
      .select("sport", "dport", "direction", "m", "sid", "features_name", "features_value")
      .inColumnFamily("sessn")

    val pld_rdd = this.sparkSession.sparkContext.hbaseTable[(Array[Byte], Option[Array[Byte]], Option[Array[Byte]], Option[Array[Byte]], Option[Array[Byte]])](input_table)
      .select("m", "sid", "direction", "payload")
      .inColumnFamily("p")

    val training_rdd = input_rdd.filter{
      features =>
        BigInt(features.m) == BigInt(fuzzyset_mark_b)
    }

    println(s"粗分类标识: ${fuzzyset_mark}")
    println(s"该粗分类中的会话数量: ${training_rdd.count}")

    val keywords_training_rdd = pld_rdd.filter{
      item =>
        item._2 != None && BigInt(item._2.get) == BigInt(fuzzyset_mark_b)
    }

    if (! admin.tableExists(TableName.valueOf(save_table)))
      createPresplitTable(save_table, "fzset")
    if (! admin.tableExists(TableName.valueOf(save_table2)))
      createPresplitTable(save_table2, "model")

    val fuzzyset_feature_rdd = FuzzySetFeatureExtract(training_rdd)

    //val fuzzyset_keywords_rdd = FuzzySetKeywordsExtract(keywords_training_rdd)
    val fuzzyset_keywords_rdd = sparkSession.sparkContext.parallelize(Seq.empty[(Array[Byte], String)])

    fuzzyset_feature_rdd.toHBaseTable(save_table)
      .toColumns("m", "id", "features_name", "features_value", "keywords")
      .inColumnFamily("fzset")
      .save()

    fuzzyset_keywords_rdd.toHBaseTable(save_table)
      .toColumns("keywords")
      .inColumnFamily("fzset")
      .save()

    val source_data_rdd = this.sparkSession.sparkContext.hbaseTable[FuzzySetTable](save_table)
      .select("m", "id", "features_name", "features_value", "keywords")
      .inColumnFamily("fzset")

    val old_model_rdd = this.sparkSession.sparkContext.hbaseTable[(Array[Byte], Option[Array[Byte]], Option[Array[String]], Option[Array[(Double, Double)]], Option[String], Option[String])](save_table2)
      .select("id", "features_name", "features_value", "fkeywords", "bkeywords")
      .inColumnFamily("model")
      .map(item => ProtoModelTable(item._1, item._2, item._3, item._4, item._5, item._6))

    val clustering_training_rdd = source_data_rdd
        .filter(item => item.m != None && BigInt(item.m.get) == BigInt(fuzzyset_mark_b))

    val (fuzzyset_rowkey_id_rdd, new_model_rdd) = ProtoModelExtract(clustering_training_rdd, old_model_rdd, protocol_name)
    val cur_fzset_proto = Bytes.toString(fuzzyset_rowkey_id_rdd.collect()(0)._2)

    println(s"推荐使用的关键词: ${fuzzyset_keywords_rdd.collect.map(_._2).toList}")
    println(s"该粗分类所属协议: ${cur_fzset_proto}")

    fuzzyset_rowkey_id_rdd.toHBaseTable(save_table)
      .toColumns("id")
      .inColumnFamily("fzset")
      .save()

    new_model_rdd.toHBaseTable(save_table2)
      .toColumns("id", "features_name", "features_value", "keywords")
      .inColumnFamily("model")
      .save()

    this.sparkSession.close()
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
