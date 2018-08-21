name := "sparknextdemo"

version := "0.1"

scalaVersion := "2.11.12"

scalacOptions += "-Ypartial-unification"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion  % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion  % Provided ,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided ,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.hadoop" % "hadoop-common" % "2.7.4",
  "org.apache.hbase" % "hbase-server" % "1.3.1",
  "org.apache.hbase" % "hbase-common" % "1.3.1" ,
  "org.apache.hbase" % "hbase-client" % "1.3.1",
  "eu.unicredit" %% "hbase-rdd" % "0.8.0",
  "org.pcap4j" % "pcap4j-core" % "1.7.2",
  "org.pcap4j" % "pcap4j-packetfactory-static" % "1.7.2",
  "org.msgpack" %% "msgpack-scala" % "0.8.13",
  "org.msgpack" % "jackson-dataformat-msgpack" % "0.8.13",
  "org.json4s" %% "json4s-jackson" % "3.2.11",
  "com.chuusai" %% "shapeless" % "2.3.3"
)

enablePlugins(PackPlugin)
