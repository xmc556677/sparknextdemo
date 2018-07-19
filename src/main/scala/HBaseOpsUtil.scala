package cc.xmccc.sparkdemo.schema

import it.nerdammer.spark.hbase.conversion._
import org.apache.hadoop.hbase.util.Bytes
import shapeless.{::, Generic, HList, HNil, Lazy, Poly, Poly1}

case class SessionFeatureTable(
                              rowkey: Array[Byte], avg_pkt_len: Int, min_pkt_len: Int, max_pkt_len: Int, var_pkt_len: Double,
                              avg_ts_IAT: BigInt, min_ts_IAT: BigInt, max_ts_IAT: BigInt, var_ts_IAT: Double,
                              avg_pld_len: Int, min_pld_len: Int, max_pld_len: Int, var_pld_len: Double,
                              total_bytes: Long, sessn_dur: BigInt, pkts_cnt: Long, psh_cnt: Long, sport: Array[Byte],
                              dport: Array[Byte], direction: Array[Byte], m: Array[Byte],

                              sc_avg_pkt_len: Int, sc_min_pkt_len: Int, sc_max_pkt_len: Int, sc_var_pkt_len: Double,
                              sc_avg_ts_IAT: BigInt, sc_min_ts_IAT: BigInt, sc_max_ts_IAT: BigInt, sc_var_ts_IAT: Double,
                              sc_avg_pld_len: Int, sc_min_pld_len: Int, sc_max_pld_len: Int, sc_var_pld_len: Double,
                              sc_total_bytes: Long, sc_pkt_cnt: Long,

                              cs_avg_pkt_len: Int, cs_min_pkt_len: Int, cs_max_pkt_len: Int, cs_var_pkt_len: Double,
                              cs_avg_ts_IAT: BigInt, cs_min_ts_IAT: BigInt, cs_max_ts_IAT: BigInt, cs_var_ts_IAT: Double,
                              cs_avg_pld_len: Int, cs_min_pld_len: Int, cs_max_pld_len: Int, cs_var_pld_len: Double,
                              cs_total_bytes: Long, cs_pkt_cnt: Long
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
                                    rowkey: Array[Byte],
                                    avg_pkt_len: Double, min_pkt_len: Double, max_pkt_len: Double, var_pkt_len: Double,
                                    avg_ts_IAT: Double, min_ts_IAT: Double, max_ts_IAT: Double, var_ts_IAT: Double,
                                    avg_pld_len: Double, min_pld_len: Double, max_pld_len: Double, var_pld_len: Double,
                                    total_bytes: Double, sessn_dur: Double, pkts_cnt: Double, psh_cnt: Double,

                                    sc_avg_pkt_len: Double, sc_min_pkt_len: Double, sc_max_pkt_len: Double, sc_var_pkt_len: Double,
                                    sc_avg_ts_IAT: Double, sc_min_ts_IAT: Double, sc_max_ts_IAT: Double, sc_var_ts_IAT: Double,
                                    sc_avg_pld_len: Double, sc_min_pld_len: Double, sc_max_pld_len: Double, sc_var_pld_len: Double,
                                    sc_total_bytes: Double, sc_pkt_cnt: Double,

                                    cs_avg_pkt_len: Double, cs_min_pkt_len: Double, cs_max_pkt_len: Double, cs_var_pkt_len: Double,
                                    cs_avg_ts_IAT: Double, cs_min_ts_IAT: Double, cs_max_ts_IAT: Double, cs_var_ts_IAT: Double,
                                    cs_avg_pld_len: Double, cs_min_pld_len: Double, cs_max_pld_len: Double, cs_var_pld_len: Double,
                                    cs_total_bytes: Double, cs_pkt_cnt: Double

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
}

import HBaseOpsUtil._

