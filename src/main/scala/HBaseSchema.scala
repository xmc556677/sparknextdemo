package cc.xmccc.sparkdemo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.sql.functions.{col, lit}

case class HBaseRecord(
                      col0: String,
                      r: String
)

object HBaseSchema {
  val cat =
    s"""{
       |"table":{"namespace": "xmc", "name": "shcTest", "tableCoder": "PrimitiveType"},
       |"rowkey": "key",
       |"columns": {
       |  "col0": {"cf": "rowkey", "col": "key", "type": "string"},
       |  "r": {"cf": "packet", "col": "r", "type": "string"},
       |  "p": {"cf": "packet", "col": "p", "type": "binary"}
       | }
       |}""".stripMargin

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("HBaseSchema")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    import sqlContext.implicits._

    val data = (0 to 100).map(
      i => HBaseRecord(i.toString, i.toString + "hello")
    )

    val df = sc.parallelize(data).toDF
    df.show(10)
    val n_df_2_a = df
      .filter(col("r").startsWith("1"))
      .withColumn("p", lit(Array(1.toByte)))

    n_df_2_a.show()

    n_df_2_a.write.options(
      Map(HBaseTableCatalog.tableCatalog -> cat,
        HBaseTableCatalog.newTable -> "5")
    ).format("org.apache.spark.sql.execution.datasources.hbase")
    .save()

    sparkSession.close()
  }
}
