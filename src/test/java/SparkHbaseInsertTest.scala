import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkHbaseInsertTest {
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
    val rdd = sparkContext.makeRDD(Array(1)).flatMap(_ => 0 to 100000)
    rdd.map(x => {
      var put = new Put(Bytes.toBytes(x.toString))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("c1"), Bytes.toBytes(x.toString))
      (new ImmutableBytesWritable, put)
    }).saveAsHadoopDataset(jobConf)
  }
}
