package cc.xmccc.sparkdemo

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy
import cc.xmccc.hbase.util.HBaseUtil
import org.apache.hadoop.hbase.util.Bytes

object HBaseAdminExample {

  //create config file from resources/hbase-site.xml and create connection from config
  val conf = HBaseConfiguration.create()
  conf.set("hbase.master", "hmaster.hbase:60000")
  val conn = ConnectionFactory.createConnection(conf)
  val admin = conn.getAdmin()

  def createTable(table_name: String, cf_name: String) = {
    val table_name_create = TableName.valueOf(table_name)
    val column_family_name = new HColumnDescriptor(cf_name)
    if (admin.tableExists(table_name_create)) {
      val table = new HTableDescriptor(table_name_create)
      table.addFamily(column_family_name)
      admin.createTable(table)
    }
  }

  def createNamespace(name: String): Unit = {
    val ns = NamespaceDescriptor.create(name).build()
    admin.createNamespace(ns)
  }

  def listTables(): Unit = {
    admin.listTableNames().foreach{
      table_name =>
        println(table_name)
    }
  }

  def listNamespaces(): Unit = {
    admin.listNamespaceDescriptors().foreach{
      namespace =>
        println(namespace.getName)
    }
  }

  def deleteTable(name: String): Unit = {
    val table_name_delete = TableName.valueOf(name)
    if (admin.tableExists(table_name_delete)) {
      admin.disableTable(table_name_delete) //disable the table first in order to delete the table
      admin.deleteTable(table_name_delete)
    }
  }

  def main(args: Array[String]): Unit = {

    //create namespace 'dummytestt'
    createNamespace("dummytestt")

    //list namespaces
    println("created namespaces: ")
    listNamespaces()

    //create table in HBase, table name: testtest, column family: test, namespace: dummytestt
    createTable("dummytestt:testtest", "test")

    //list table in HBase
    println("created tables")
    listTables()

    //delete table in HBase
    deleteTable("dummytestt:testtest")

    //list table in HBase
    println("created tables")
    listTables()

    //create table with rowkey presplit(load balance)
    /* rowkey   regionserver
    *  0 - 8         0
    *  9 - 16        1
    *  17 - 24       2
    *  25 - 32       3
    *  33 - 40       4
    *       .....
    * */
    val table_name_create_presplit = TableName.valueOf("dummytestt:rowkey_presplit")
    val table_presplit = new HTableDescriptor(table_name_create_presplit)
    table_presplit.addFamily(new HColumnDescriptor("test"))
    //config table to presplit rowkey
    table_presplit.setValue(HTableDescriptor.SPLIT_POLICY, classOf[KeyPrefixRegionSplitPolicy].getName)
    //set the prefix length of rowkey for presplit
    table_presplit.setValue(KeyPrefixRegionSplitPolicy.PREFIX_LENGTH_KEY, "4")

    admin.createTable(
      table_presplit,
      HBaseUtil.getHexSplits(
        Bytes.toBytes(0.toInt),
        Bytes.toBytes(120.toInt),
        15
      )
    )

    //delete presplit table
    deleteTable("dummytestt:rowkey_presplit")

    //delete namespace
    admin.deleteNamespace("dummytestt")

    //list namespaces
    println("created namespaces: ")
    listNamespaces()

    //close connection
    admin.close()
    conn.close()

  }
}
