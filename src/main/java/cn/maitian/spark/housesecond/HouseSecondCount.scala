package cn.maitian.spark.housesecond

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object HouseSecondCount {
  case class houseSecond( client:String,city:String, keyword:String,area:String)
  def main(args: Array[String]) {
    //val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql")
    //val sparkConf = new SparkConf().setMaster("spark://master:7077").setAppName("SparkSql")
    //val sc  = new SparkContext(sparkConf)
    //val ssc = new StreamingContext(sc, Seconds(10))
    //val sqlContext = new SQLContext(sc)
    val spark =
      SparkSession
        .builder()
        .appName("SparkSql HouseSecondCount")
        //.master("spark://172.16.11.51:7077 ")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
    //val houseseconddata = sc.textFile("/maitian/HouseSecond/HouseSecond*")
    /*val houseseconddata = sc.sequenceFile[UserID, UserInfo]("hdfs://master:9000/maitian/HouseSecond/HouseSecond*")
      .partitionBy(new HashPartitioner(100)) // 构造100个分区
      .persist()*/
    val houseseconddata = spark.read.textFile("/maitian/HouseSecond/*")
    val housesecond = houseseconddata.rdd.map (
      line =>  line.split(",")).filter(_.length>20).map(data=>
      houseSecond(data(0),data(1),data(2).replaceAll("\"", ""),data(15)) )

    /*val housesecond=houseseconddata.map(line=>line.split(",")).map{
      s =>s.length>20
        houseSecond(s(0),s(1),s(2).replaceAll("\"", ""),s(15))
    }*/
    import spark.implicits._
    val  dfhousesecond= housesecond.toDF()
    dfhousesecond.createOrReplaceTempView("housesecond")
    //dfhousesecond.show()
    //val sqlDF = spark.sql("SELECT * FROM housesecond")
    val sqlDF = spark.sql("SELECT city,pv FROM (SELECT city,count(*) pv FROM housesecond   GROUP BY city) t ORDER BY pv DESC")
    sqlDF.show()
    spark.stop()
  }
}
