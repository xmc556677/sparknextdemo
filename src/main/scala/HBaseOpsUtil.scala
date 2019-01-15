package cc.xmccc.sparkdemo.schema

import it.nerdammer.spark.hbase.conversion._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.execution.datasources.hbase.RowKey
import shapeless.{::, Generic, HList, HNil, Lazy, Poly, Poly1}

case class SessionFeatureTable(
                              rowkey: Array[Byte], avg_pkt_len: Int, min_pkt_len: Int, max_pkt_len: Int, var_pkt_len: Double,
                              avg_ts_IAT: BigInt, min_ts_IAT: BigInt, max_ts_IAT: BigInt, var_ts_IAT: Double,
                              avg_pld_len: Int, min_pld_len: Int, max_pld_len: Int, var_pld_len: Double,
                              total_bytes: Long, sessn_dur: BigInt, pkts_cnt: Long, psh_cnt: Long, sport: Array[Byte],
                              dport: Array[Byte], direction: Array[Byte], m: Array[Byte], sid: Array[Byte],

                              sc_avg_pkt_len: Int, sc_min_pkt_len: Int, sc_max_pkt_len: Int, sc_var_pkt_len: Double,
                              sc_avg_ts_IAT: BigInt, sc_min_ts_IAT: BigInt, sc_max_ts_IAT: BigInt, sc_var_ts_IAT: Double,
                              sc_avg_pld_len: Int, sc_min_pld_len: Int, sc_max_pld_len: Int, sc_var_pld_len: Double,
                              sc_total_bytes: Long, sc_pkt_cnt: Long,

                              cs_avg_pkt_len: Int, cs_min_pkt_len: Int, cs_max_pkt_len: Int, cs_var_pkt_len: Double,
                              cs_avg_ts_IAT: BigInt, cs_min_ts_IAT: BigInt, cs_max_ts_IAT: BigInt, cs_var_ts_IAT: Double,
                              cs_avg_pld_len: Int, cs_min_pld_len: Int, cs_max_pld_len: Int, cs_var_pld_len: Double,
                              cs_total_bytes: Long, cs_pkt_cnt: Long
                              )

case class NewSessionFeatureTable(
                                rowkey: Array[Byte],
                                sport: Array[Byte],
                                dport: Array[Byte],
                                direction: Array[Byte],
                                m: Array[Byte],
                                sid: Array[Byte],
                                features_name: Array[String],
                                features_value: Array[Double]
                              )

case class SessionFeatureToExtract(
                                rowkey: Array[Byte], m: Array[Byte], avg_pkt_len: Int, min_pkt_len: Int, max_pkt_len: Int, var_pkt_len: Double,
                                avg_ts_IAT: BigInt, min_ts_IAT: BigInt, max_ts_IAT: BigInt, var_ts_IAT: Double,
                                avg_pld_len: Int, min_pld_len: Int, max_pld_len: Int, var_pld_len: Double,
                                total_bytes: Long, sessn_dur: BigInt, pkts_cnt: Long, psh_cnt: Long,

                                sc_avg_pkt_len: Int, sc_min_pkt_len: Int, sc_max_pkt_len: Int, sc_var_pkt_len: Double,
                                sc_avg_ts_IAT: BigInt, sc_min_ts_IAT: BigInt, sc_max_ts_IAT: BigInt, sc_var_ts_IAT: Double,
                                sc_avg_pld_len: Int, sc_min_pld_len: Int, sc_max_pld_len: Int, sc_var_pld_len: Double,
                                sc_total_bytes: Long, sc_pkt_cnt: Long,

                                cs_avg_pkt_len: Int, cs_min_pkt_len: Int, cs_max_pkt_len: Int, cs_var_pkt_len: Double,
                                cs_avg_ts_IAT: BigInt, cs_min_ts_IAT: BigInt, cs_max_ts_IAT: BigInt, cs_var_ts_IAT: Double,
                                cs_avg_pld_len: Int, cs_min_pld_len: Int, cs_max_pld_len: Int, cs_var_pld_len: Double,
                                cs_total_bytes: Long, cs_pkt_cnt: Long
                              )

case class FuzzySetAvgFeatureTable(
                                    rowkey: Array[Byte], m: Array[Byte],
                                    avg_pkt_len: (Double, Double), min_pkt_len: (Double, Double), max_pkt_len: (Double, Double), var_pkt_len: (Double, Double),
                                    avg_ts_IAT: (Double, Double), min_ts_IAT: (Double, Double), max_ts_IAT: (Double, Double), var_ts_IAT: (Double, Double),
                                    avg_pld_len: (Double, Double), min_pld_len: (Double, Double), max_pld_len: (Double, Double), var_pld_len: (Double, Double),
                                    total_bytes: (Double, Double), sessn_dur: (Double, Double), pkts_cnt: (Double, Double), psh_cnt: (Double, Double),

                                    sc_avg_pkt_len: (Double, Double), sc_min_pkt_len: (Double, Double), sc_max_pkt_len: (Double, Double), sc_var_pkt_len: (Double, Double),
                                    sc_avg_ts_IAT: (Double, Double), sc_min_ts_IAT: (Double, Double), sc_max_ts_IAT: (Double, Double), sc_var_ts_IAT: (Double, Double),
                                    sc_avg_pld_len: (Double, Double), sc_min_pld_len: (Double, Double), sc_max_pld_len: (Double, Double), sc_var_pld_len: (Double, Double),
                                    sc_total_bytes: (Double, Double), sc_pkt_cnt: (Double, Double),

                                    cs_avg_pkt_len: (Double, Double), cs_min_pkt_len: (Double, Double), cs_max_pkt_len: (Double, Double), cs_var_pkt_len: (Double, Double),
                                    cs_avg_ts_IAT: (Double, Double), cs_min_ts_IAT: (Double, Double), cs_max_ts_IAT: (Double, Double), cs_var_ts_IAT: (Double, Double),
                                    cs_avg_pld_len: (Double, Double), cs_min_pld_len: (Double, Double), cs_max_pld_len: (Double, Double), cs_var_pld_len: (Double, Double),
                                    cs_total_bytes: (Double, Double), cs_pkt_cnt: (Double, Double)
                                  )

case class ProtoModelTable(
                            rowkey: Array[Byte], id: Option[Array[Byte]],
                            features_name: Option[Array[String]],
                            features_value: Option[Array[(Double, Double)]],
                            fkeywords: Option[String],
                            bkeywords: Option[String]
                          )

case class FuzzySetTable(
                        rowkey: Array[Byte], m: Option[Array[Byte]], id: Option[Array[Byte]],
                        features_name: Option[Array[String]],
                        features_value: Option[Array[(Double, Double)]],
                        fkeywords: Option[String],
                        bkeywords: Option[String]
                        )

object HBaseOpsUtil {

  implicit def hnilReader: FieldReader[HNil] =
    new FieldReader[HNil] {
      def map(data: HBaseData): HNil = HNil
    }

  implicit def hlistReader[H, T <: HList](
                          implicit
                          hReader: Lazy[FieldReader[H]],
                          tReader: FieldReader[T]
                          ): FieldReader[H :: T] =
    new FieldReader[H :: T] {
      def map(data: HBaseData): H :: T = {
        val head = data.take(1)
        val tail = data.drop(1)
        hReader.value.map(head) :: tReader.map(tail)
      }
    }

  implicit def genericReader[A, R](
                                  implicit
                                  gen: Generic.Aux[A, R],
                                  reader: Lazy[FieldReader[R]]
                                  ): FieldReader[A] =
    new FieldReader[A] {
      def map(data: HBaseData): A = {
        gen.from(reader.value.map(data))
      }
    }

  implicit def bigintReader: FieldReader[BigInt] = new SingleColumnConcreteFieldReader[BigInt] {
    override def columnMap(cols: Array[Byte]): BigInt = BigInt(Array(0.toByte) ++ cols)
  }

  implicit def doublearrayReader: FieldReader[Array[Double]] = new SingleColumnConcreteFieldReader[Array[Double]] {
    override def columnMap(cols: Array[Byte]): Array[Double] =
      (0 to ((cols.length - 1) / 8))
        .map(i => Bytes.toDouble(cols.slice(i * 8, (i + 1) * 8))).toArray
  }

  implicit def stringarrayReader: FieldReader[Array[String]] = new SingleColumnConcreteFieldReader[Array[String]] {
    override def columnMap(cols: Array[Byte]): Array[String] =
      Bytes.toString(cols).split(',')
  }

  implicit def tuple2doublearrayReader: FieldReader[Array[(Double, Double)]] = new SingleColumnConcreteFieldReader[Array[(Double, Double)]] {
    override def columnMap(cols: Array[Byte]): Array[(Double, Double)] = {
      (0 to ((cols.length - 1) / 16)).map(_ * 2)
        .map {
          i =>
            val i1 = cols.slice(i * 8, (i + 1) * 8)
            val i2 = cols.slice((i + 1) * 8, (i + 2) * 8)
            (Bytes.toDouble(i1), Bytes.toDouble(i2))
        }.toArray
    }
  }

  implicit def tuple2doubleReader: FieldReader[(Double, Double)] = new SingleColumnConcreteFieldReader[(Double, Double)] {
    override def columnMap(cols: Array[Byte]): (Double, Double) =
      (Bytes.toDouble(cols.slice(0, 8)), Bytes.toDouble(cols.slice(8, 16)))
  }

  implicit def hnilWriter: FieldWriter[HNil] =
    new FieldWriter[HNil] {
      def map(data: HNil): HBaseData = {
        Iterable.empty[Option[Array[Byte]]]
      }
    }

  implicit def hlistWriter[H, T <: HList](
                                         implicit
                                         hWriter: Lazy[FieldWriter[H]],
                                         tWriter: FieldWriter[T]
                                         ): FieldWriter[H :: T] =
    new FieldWriter[H :: T] {
      def map(data: H :: T): HBaseData = {
        val head = data.head
        val tail = data.tail
        hWriter.value.map(head) ++ tWriter.map(tail)
      }
    }

  implicit def genericWriter[A, R](
                                  implicit
                                  gen: Generic.Aux[A, R],
                                  writer: Lazy[FieldWriter[R]]
                                  ): FieldWriter[A] =
    new FieldWriter[A] {
      def map(data: A): HBaseData =
        writer.value.map(gen.to(data))
    }

  implicit def bigintWriter: FieldWriter[BigInt] = new SingleColumnFieldWriter[BigInt] {
    override def mapColumn(data: BigInt): Option[Array[Byte]] = Some(Bytes.toBytes(data.toLong))
  }

  implicit def tuple2doubleWriter: FieldWriter[(Double, Double)] = new SingleColumnFieldWriter[(Double, Double)] {
    override def mapColumn(data: (Double, Double)): Option[Array[Byte]] =
      Some(Bytes.toBytes(data._1) ++ Bytes.toBytes(data._2))
  }

  implicit def tuple2doublearrayWriter: FieldWriter[Array[(Double, Double)]] = new SingleColumnFieldWriter[Array[(Double, Double)]] {
    override def mapColumn(data: Array[(Double, Double)]): Option[Array[Byte]] =
      Some(data.map(i => Bytes.toBytes(i._1) ++ Bytes.toBytes(i._2)).flatten)
  }

  implicit def doublearrayWriter: FieldWriter[Array[Double]] = new SingleColumnFieldWriter[Array[Double]] {
    override def mapColumn(data: Array[Double]): Option[Array[Byte]] =
      Some(data.map(d => Bytes.toBytes(d)).flatten)
  }

  implicit def stringarrayWriter: FieldWriter[Array[String]] = new SingleColumnFieldWriter[Array[String]] {
    override def mapColumn(data: Array[String]): Option[Array[Byte]] =
      Some(Bytes.toBytes(data.mkString(",")))
  }
}

import HBaseOpsUtil._

