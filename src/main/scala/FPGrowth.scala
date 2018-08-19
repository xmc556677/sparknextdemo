package cc.xmccc.sparkdemo

import java.security.MessageDigest

import org.apache.spark.sql.SparkSession
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.mllib.fpm.FPGrowth
import cc.xmccc.sparkdemo.Utils.repr_string
import cc.xmccc.sparkdemo.schema.HBaseOpsUtil._
import org.apache.spark.rdd.RDD

object FPGrowth {
  def slicePacket(str: Array[Byte], n: Int): List[Array[Byte]] =
    (0 to str.length - n).map(i => str.slice(i, i+n)).toList

  def stringToByteArray(str: String): Array[Byte] = {
    str.toCharArray.map(_.toByte)
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("FPGrowth")
      .getOrCreate()

    val input_table = args(0)
    val save_table = args(1)

    val input_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Option[Array[Byte]], Option[Array[Byte]], Option[Array[Byte]], Option[Array[Byte]])](input_table)
      .select("fm", "sid", "direction", "payload")
      .inColumnFamily("p")

    val sessn_pkts_rdd = input_rdd.filter{
      case(_, Some(_), Some(_), Some(_), _) => true
      case _ => false
    }

    println(sessn_pkts_rdd.count())

    val fuzzyset_direction_sessn_rdd = sessn_pkts_rdd.groupBy{
      case(_, Some(fuzzyset_mark), Some(session_mark), Some(direction), _) =>
        (BigInt(fuzzyset_mark), BigInt(session_mark), BigInt(direction))
    }

    val fuzzyset_directions = sessn_pkts_rdd.groupBy{
      case(_, Some(fuzzyset_mark), Some(session_mark), Some(direction), _) =>
        (BigInt(fuzzyset_mark), BigInt(direction))
    }.map{case((fuzzy_mark, direction), _) => (fuzzy_mark, direction)}.collect()

    println(fuzzyset_directions.length)

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

        println("datas: ")
        println(s"length: ${fuzzyset_direction_rdd.count()}")
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

    save_rdd.toHBaseTable(save_table)
      .toColumns("keywords")
      .inColumnFamily("avg")
      .save()

    sparkSession.close()
  }
}
