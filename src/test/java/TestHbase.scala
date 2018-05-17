
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin}

import org.apache.hadoop.hbase.protobuf.generated.CellProtos.KeyValue
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos.{FamilyFilter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.util.Bytes


object TestHbase {
  def main(args: Array[String]): Unit = {

    //    ExecutorService pool = Executors.newFixedThreadPool(10)
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "master")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val hBaseUtils = new HbaseUtils()
    val admin = hBaseUtils.getAdmin(conf)
    //    val list = List("family1","family2")
    //    hBaseUtils.createTable(admin,"test2",list)


    //    println(hBaseUtils.insertData(conf,"test2","rowkey1","family1","李四","lisi2"))
    //    val row = hBaseUtils.getRow(conf,"test2","rowkey1")
    //    row.foreach(a=>{print(new String(a.getRow())+" ");print(a.getTimestamp+" ");print(new String(a.getFamily)+" ");print(new String(a.getValue)+" ")})

    //    print(hBaseUtils.delRow(conf,"test2","rowkey1"))
    val all = hBaseUtils.getByScan(conf,"test2")
    //    all.foreach(a=>a.foreach( a=>{print(new String(a.getRow())+" ");print(a.getTimestamp+" ")
    //      ;print(new String(a.getFamily)+" ");print(new String(a.getValue)+" ")}))
    all.foreach(a=>a.foreach( a=>{print(new String(a.getRowArray,a.getRowOffset,a.getRowLength)+"-->row  ");
      print(a.getTimestamp+"-->timpsstamp  ");
      print(new String(a.getFamilyArray,a.getFamilyOffset,a.getFamilyLength)+"-->family  ");
      println(new String(a.getValueArray,a.getValueOffset,a.getValueLength)+"-->value  "+
        new String(a.getQualifierArray,a.getQualifierOffset,a.getQualifierLength)+ " -->Tags")}))
    //    print(hBaseUtils.updateByDelete(conf,"test2","rowkey1","family1","李四","equal","lisi2","小王八蛋"))
    //    print(hBaseUtils.updateByCover(conf,"test2","rowkey1","family1","李四","equal","小王八蛋","小王八蛋2"))
    //    val row = hBaseUtils.findByOldTime(conf,"test2","rowkey1","family2","李四",1501810811243L).getRow
    //    print(row)v

  }
}
