package cc.xmccc.sparkdemo

import cc.xmccc.hbase.util.HBaseUtil
import cc.xmccc.sparkdemo.schema.{FuzzySetTable, ProtoModelTable}
import it.nerdammer.spark.hbase._
import cc.xmccc.sparkdemo.schema.HBaseOpsUtil._
import cc.xmccc.sparkdemo.schema._
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ExtractFuzzySetProtoModel {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.master", "hmaster.hbase:60000")
  val conn = ConnectionFactory.createConnection(conf)
  val admin = conn.getAdmin()

  val sparkSession = SparkSession.builder()
    .appName("ExtractFuzzySetProtoModel")
    .getOrCreate()

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
    val save_table = args(1)
    val fuzzyset_mark = args(2)
    val protocol_name = args(3)

    val fuzzyset_mark_b = (0 to fuzzyset_mark.length-1 by 2).map{
      i =>
        val hex = fuzzyset_mark.slice(i, i+2)
        Integer.parseInt(hex, 16).toByte
    }.toArray

    val source_data_rdd = this.sparkSession.sparkContext.hbaseTable[FuzzySetTable](input_table)
      .select("m", "id", "features_name", "features_value", "fkeywords", "bkeywords")
      .inColumnFamily("fzset")

    if (! admin.tableExists(TableName.valueOf(save_table)))
      createPresplitTable(save_table, "model")

    val old_model_rdd = this.sparkSession.sparkContext.hbaseTable[(Array[Byte], Option[Array[Byte]], Option[Array[String]], Option[Array[(Double, Double)]], Option[String], Option[String])](save_table)
      .select("id", "features_name", "features_value", "fkeywords", "bkeywords")
      .inColumnFamily("model")
      .map(item => ProtoModelTable(item._1, item._2, item._3, item._4, item._5, item._6))

    val clustering_training_rdd = source_data_rdd
      .filter(item => item.m != None && BigInt(item.m.get) == BigInt(fuzzyset_mark_b))

    val save_rdd = clustering_training_rdd.map{
      case FuzzySetTable(rowkey, m, id, features_name, features_value, fkeywords, bkeywords) =>
        val id = Bytes.toBytes(protocol_name)
        ProtoModelTable(id, Some(id), features_name, features_value, fkeywords, bkeywords)
    }

    save_rdd.toHBaseTable(save_table)
      .toColumns("id", "features_name", "features_value", "fkeywords", "bkeywords")
      .inColumnFamily("model")
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
