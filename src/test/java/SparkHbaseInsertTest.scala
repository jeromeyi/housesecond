import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.hhl.hbase.RowKey

object SparkHbaseInsertTest extends RowKey{
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master")
    var masterUrl = "local[1]"
    if (args.length > 0) {
      masterUrl = args(0)
    }
    val sconf = new SparkConf().setMaster(masterUrl).setAppName("HbaseInsertTest")
    val sparkContext = new SparkContext(sconf)
    val conf = HBaseConfiguration.create()
    var jobConf = new JobConf(conf)
    jobConf.set("hbase.zookeeper.quorum", "master,slave1,slave2,slave3,slave4")
    //jobConf.set("zookeeper.znode.parent", "/hbase")
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "test")
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    //val rdd = sparkContext.makeRDD(Array(1)).flatMap(_ => 0 to 100000)
    val rdd = sparkContext.makeRDD(Array(1)).flatMap(_ => 1 to 100000000)
    rdd.map(x => {
      //MD5Hash.getMD5AsHex(Bytes.toBytes("之前的rowkey前缀"))
      //var put = new Put(Bytes.toBytes(x.toString))
      //var put = new Put(Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes.toBytes(x.toString))))
     /* var put = new Put(Bytes.add(MD5Hash.getMD5AsHex(Bytes.toBytes(x)).substring(0, 8).getBytes(),
        Bytes.toBytes(x)));*/
      //var put = new Put(rowKeyByMD5(x.toString))
      var put = new Put(rowKeyWithMD5Prefix("_",0,x.toString))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("c1"), Bytes.toBytes(x.toString))
      (new ImmutableBytesWritable, put)
    }).saveAsHadoopDataset(jobConf)
  }

  /*def rowKeyByMD5(column: String*): Array[Byte] = {
    val rkString = column.mkString("")
    val md5 = MD5Hash.getMD5AsHex(Bytes.toBytes(rkString))
    val rowKey = Bytes.toBytes(md5)
    rowKey
  }*/
}
