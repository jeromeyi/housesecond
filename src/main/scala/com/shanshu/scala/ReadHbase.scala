package com.shanshu.scala

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object ReadHbase {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master")
    val conf = HBaseConfiguration.create()

    conf.set("hbase_zookeeper_property_clientPort","2181")
    conf.set("hbase_zookeeper_quorum","master,slave1,slave2,slave3,slave4")

    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("readHbase")

    val sc = new SparkContext(sparkConf)
    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "fruit")
    val stuRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //遍历输出
    stuRDD.foreach({ case (_,result) =>
      val key = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue("info".getBytes,"name".getBytes))
      val color = Bytes.toString(result.getValue("info".getBytes,"color".getBytes))
      val num = Bytes.toString(result.getValue("info".getBytes,"num".getBytes))
      val people = Bytes.toString(result.getValue("info".getBytes,"people".getBytes))
      println("Row key:"+key+" Name:"+name+" color:"+color+" num:"+num+" people"+people)
    })
    sc.stop()
  }
}
