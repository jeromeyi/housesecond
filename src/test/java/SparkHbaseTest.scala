import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io._
import org.hhl.hbase.HbaseHelper

import scala.collection.mutable.ListBuffer
object SparkHbaseTest {
  var nameSpace = "default"
  case class MD( key:String,value:String)
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master")
    var masterUrl = "local[1]"
    if (args.length > 0) {
      masterUrl = args(0)
    }
    val sconf = new SparkConf().setMaster(masterUrl).setAppName("SparkHbaseTest")
    val sparkContext = new SparkContext(sconf)
    val tablename = "salecontract"

    val conf = HBaseConfiguration.create()

    conf.set("hbase.zookeeper.quorum","master,slave1,slave2,slave3,slave4")

    conf.set("hbase.zookeeper.property.clientPort", "2181")

    conf.set(TableInputFormat.INPUT_TABLE, tablename)

    //val admin = new HBaseAdmin(conf)
    val connection = ConnectionFactory.createConnection(conf)
    //deleteHTable(connection,tablename)
    //return ;
    val admin =connection.getAdmin().asInstanceOf[HBaseAdmin]

    if (!admin.isTableAvailable(tablename)) {

      //val tableDesc = new HTableDescriptor(TableName.valueOf(tablename))

      //admin.createTable(tableDesc)
      val columnFamily = Seq("ci")


      new HbaseHelper().createHTable(connection,tablename,20,columnFamily.toArray)

    }
    return;
    val  table = connection.getTable(TableName.valueOf(tablename))
    val get = new Get(Bytes.toBytes("fb038c3e_35989"))
    val result: Result  = table.get(get)
   /* val cells=result.rawCells()
    val c1IdsList = new ListBuffer[String]();
    for(cell <- cells){
      val family = Bytes.toString(CellUtil.cloneFamily(cell));
      println("family---"+family)
      if("cf".equals(family)) {
        val c1 = Bytes.toString(CellUtil.cloneQualifier(cell));
        c1IdsList+=c1;
       // println(c1.getBytes)
      }
    //System.out.println(" 列：" + new String(CellUtil.cloneFamily(keyValue))+":"+new String(CellUtil.cloneQualifier(keyValue)) + "     值:" + new String(CellUtil.cloneValue(keyValue)));
  }
  */
      println(Bytes.toString(result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("c1"))))


    return


    val hBaseRDD = sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat],

      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],

      classOf[org.apache.hadoop.hbase.client.Result])

    val count = hBaseRDD.count()
    val datas = hBaseRDD.map( x=>x._2).map{
      result => (result.getRow,result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("c1")))
    }.map(row => (new String(row._1),new String(row._2)))
      .collect.foreach(r => (println(r._1+":"+r._2)))
    hBaseRDD.foreach{case (_,result) =>{

      val rowKey = Bytes.toString(result.getRow)

      val value= Bytes.toString(result.getValue("cf".getBytes,"c1".getBytes))

      println("rowKey:"+rowKey+" Value:"+value)

    }}
  }

  def deleteHTable(connection: Connection, tn: String): Unit = {
    val tableName = TableName.valueOf(nameSpace + ":" + tn)
    val admin = connection.getAdmin
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }
  }
}
