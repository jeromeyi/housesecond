import java.util

import com.google.protobuf.GeneratedMessage
import org.apache.commons.codec.language.bm.Lang
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{CompareFilter, Filter, FilterBase}
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.conf.Configuration
import scala.collection.mutable.ArrayBuffer



/**
  * Created by Administrator on 2017/8/3.  
  */
class HbaseUtils {
  val comp = new util.HashMap[String,CompareFilter.CompareOp]()
  comp.put("EQUAL",CompareFilter.CompareOp.EQUAL)
  comp.put("GREATER",CompareFilter.CompareOp.GREATER)
  comp.put("GREATER_OR_EQUAL",CompareFilter.CompareOp.GREATER_OR_EQUAL)
  comp.put("LESS",CompareFilter.CompareOp.LESS)
  comp.put("LESS_OR_EQUAL",CompareFilter.CompareOp.LESS_OR_EQUAL)
  comp.put("NO_OP",CompareFilter.CompareOp.NO_OP)
  comp.put("NOT_EQUAL",CompareFilter.CompareOp.NOT_EQUAL)

  def getAdmin(conf:Configuration):HBaseAdmin ={
    val connection = ConnectionFactory.createConnection(conf)
    connection.getAdmin().asInstanceOf[HBaseAdmin]
  }

  /**
    * @see 根据指定的管理员，表名，列族名称创建表  
    * @param admin HBaseAdmin 创建的HBaseAdmin对象  
    * @param tName String 需要创建的表名  
    * @param columnNames List[String] 列族名称的集合  
    */
  def createTable(admin: HBaseAdmin,tName:String,columnNames:List[String] ):Boolean = {
    if(admin.tableExists(tName)) return false
    try
    {val tableDesc = new HTableDescriptor(TableName.valueOf(tName))
    columnNames.foreach(columnName => tableDesc.addFamily(new HColumnDescriptor(columnName)))//添加列
    admin.createTable(tableDesc)
    true}
    catch {case e: Exception => e.printStackTrace();false}
  }

  /**
    * @see 根据表名、rowkey、列族名、列名、值、增加数据、如果增加成功则返回true  
    * @param conf HBaseConfiguration  
    * @param tableName String 表名  
    * @param rowKey String  
    * @param columnFamily String 列族名称  
    * @param column String列名  
    * @param value String 值  
    * @return 是否插入成功  
    */
  def insertData(conf:Configuration,tableName:String,rowKey:String,columnFamily:String,column:String,value:String):Boolean={
    val  table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName))
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value))
    //      put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value))
    table.put(put)
    true
  }
  def deleteTable(conf:Configuration,tableName:String):Boolean={
    val admin = getAdmin(conf)
    try{
      if(admin.tableExists(tableName)){
        admin.disableTable(tableName)
        admin.deleteTable(tableName)
      }
    }catch {
      case e:Exception=>e.printStackTrace() ; false
    }
    true
  }

  /**
    * @see 根据指定的配置信息全表扫描指定的表  
    * @param conf Configuration 配置信息  
    * @param tableName String 表名  
    * @return ArrayBuffer[Array[Cell] ]  
    */
  def getByScan(conf:Configuration,tableName:String):ArrayBuffer[Array[Cell]]={
    var arrBuffer = ArrayBuffer[Array[Cell]]()

    val scaner = new Scan()
    //      val table = new HTable(conf,tableName)
    val  table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName))
    val results = table.getScanner(scaner)
    var res:Result = results.next()
    while (res != null){
      arrBuffer += res.rawCells()
      res = results.next()
    }
    arrBuffer
  }
  def getRow(conf:Configuration,tableName:String,row:String):Array[Cell]={
    //    val table = new HTable(conf,tableName)
    val  table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(row))
    val res = table.get(get)
    res.rawCells()
  }

  def getRowResult(conf:Configuration,tableName:String,row:String):Result={
    //    val table = new HTable(conf,tableName)
    val  table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(row))
    table.get(get)
  }

  /**
    * @see 删除指定表的指定row数据  
    * @param conf Configuration  
    * @param tableName String 要删除的表名  
    * @param row String 要删除的row名  
    * @return  Boolean 是否成功  
    */
  def delRow(conf:Configuration,tableName:String,row:String):Unit={
    val  table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName))
    table.delete( new Delete(Bytes.toBytes(row)))

  }

  /**
    * @deprecated
    * @see 更新某个指定的数据  
    * @param conf Configuration  
    * @param tableName String 要更新的表名  
    * @param row String 需要更新的row  
    * @param family String 需要更新的列族  
    * @param qualifier String 需要修改的列  
    * @param compareon String   只能为，不分大小写，EQUAL、GREATER、GREATER_OR_EQUAL、LESS、LESS_OR_EQUAL、NO_OP、NOT_EQUAL  
    * @param value String 需要更改的值  
    * @param newvalue String  
    * @return
    */
  def updateByDelete(conf:Configuration,tableName:String,row:String,family:String,qualifier:String, compareon:String,value:String,newvalue:String):Boolean ={
    if(comp.get(compareon.toUpperCase)== null)false
    val  table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName))
    val mut = new RowMutations(Bytes.toBytes(row))
    mut.add(new Delete(Bytes.toBytes(row)).addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier)))
    mut.add(new Put(Bytes.toBytes(row)).addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(newvalue)))
    print("-----------" + comp.get(compareon.toUpperCase()))
    table.checkAndMutate(Bytes.toBytes(row),Bytes.toBytes(family),Bytes.toBytes(qualifier),comp.get(compareon.toUpperCase()) ,Bytes.toBytes(value),mut)
  }
  def updateByCover(conf:Configuration,tableName:String,row:String,family:String,qualifier:String, compareon:String,value:String,newvalue:String):Boolean = {
    if(comp.get(compareon.toUpperCase) == null)false
    val table =ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName))
    val mut = new RowMutations(Bytes.toBytes(row))
    mut.add(new Put(Bytes.toBytes(row)).addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(newvalue)))
    table.checkAndMutate(Bytes.toBytes(row),Bytes.toBytes(family),Bytes.toBytes(qualifier),comp.get(compareon.toUpperCase()) ,Bytes.toBytes(value),mut)
  }
  def findByOldTime(conf:Configuration,tableName:String,row:String,cFamily:String,qualifier:String, timestamp: Long): Result ={
    val table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(row))
    get.setTimeStamp(timestamp)
    get.addColumn(Bytes.toBytes(cFamily),Bytes.toBytes(qualifier))
    table.get(get)
  }

}