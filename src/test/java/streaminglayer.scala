import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import net.sf.json.JSONObject
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.zookeeper.data.ACL
import org.apache.zookeeper.data.Id
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable.ListBuffer
import scala.concurrent.JavaConversions
import org.apache.zookeeper.ZooDefs
import redis.clients.jedis.JedisPool

import scala.collection.JavaConversions._


object streaminglayer {
  Logger.getLogger("org").setLevel(Level.ERROR)
  //
  def readOffsets(topics: Seq[String], groupId:String,zkUtils:ZkUtils):

  //val zkUtils: ZkUtils = ZkUtils.apply(zkServers, ZK_TIMEOUT_MSEC, ZK_TIMEOUT_MSEC, false)  
  Map[TopicPartition, Long] = {

    val topicPartOffsetMap = collection.mutable.HashMap.empty[TopicPartition, Long]
    val partitionMap = zkUtils.getPartitionsForTopics(topics)

    // /consumers/<groupId>/offsets/<topic>/  
    partitionMap.foreach(topicPartitions => {
      val zkGroupTopicDirs = new ZKGroupTopicDirs(groupId, topicPartitions._1)
      topicPartitions._2.foreach(partition => {
        val offsetPath = zkGroupTopicDirs.consumerOffsetDir + "/" + partition

        try {
          val offsetStatTuple = zkUtils.readData(offsetPath)
          if (offsetStatTuple != null) {
            //LOGGER.info("retrieving offset details - topic: {}, partition: {}, offset: {}, node path: {}", Seq[AnyRef](topicPartitions._1, partition.toString, offsetStatTuple._1, offsetPath): _*)  

            topicPartOffsetMap.put(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)),
              offsetStatTuple._1.toLong)
          }

        } catch {
          case e: Exception =>
            //LOGGER.warn("retrieving offset details - no previous node exists:" + " {}, topic: {}, partition: {}, node path: {}", Seq[AnyRef](e.getMessage, topicPartitions._1, partition.toString, offsetPath): _*)  

            topicPartOffsetMap.put(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)), 0L)
        }
      })
    })

    topicPartOffsetMap.toMap
  }


  def persistOffsets(offsets: Seq[OffsetRange], groupId: String, storeEndOffset: Boolean,zkUtils:ZkUtils): Unit = {
    offsets.foreach(or => {
      val zkGroupTopicDirs = new ZKGroupTopicDirs(groupId, or.topic);

      val acls = new ListBuffer[ACL]()
      val acl = new ACL
      acl.setId(ZooDefs.Ids.ANYONE_ID_UNSAFE)
      acl.setPerms(ZooDefs.Perms.ALL)
      acls += acl

      val offsetPath = zkGroupTopicDirs.consumerOffsetDir + "/" + or.partition;
      val offsetVal = if (storeEndOffset) or.untilOffset else or.fromOffset
      //zkUtils.updatePersistentPath(zkGroupTopicDirs.consumerOffsetDir + "/"  
      //  + or.partition, offsetVal + "", JavaConversions.bufferAsJavaList(acls))  
      zkUtils.updatePersistentPath(zkGroupTopicDirs.consumerOffsetDir + "/"
        + or.partition, offsetVal + "", acls.toList)

      //LOGGER.debug("persisting offset details - topic: {}, partition: {}, offset: {}, node path: {}", Seq[AnyRef](or.topic, or.partition.toString, offsetVal.toString, offsetPath): _*)  
    })
  }
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master")
    val sparkConf = new SparkConf().setAppName("example")
      .setMaster("local[4]") //Uncomment this line to test while developing on a workstation
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "172.16.11.51:9092,172.16.11.52:9092,172.16.11.53:9092,172.16.11.54:9092,172.16.11.55:9092",
      //"bootstrap.servers" -> "master:9092,slave1:9092,slave2:9092,slave3:9092,slave4:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //val topics = Array("topicA", "topicB")
    val topics = Array("user_events_10")
    val zkUrl = "master:2181,slave1:2181,slave2:2181,slave3:2181,slave4:2181"
    val sessionTimeout = 6000
    //val connectionTimeout = args(2).toInt
    val connectionTimeout = 6000

    val zkClientAndConnection = ZkUtils.createZkClientAndConnection(zkUrl, sessionTimeout, connectionTimeout)
    val zkUtils = new ZkUtils(zkClientAndConnection._1, zkClientAndConnection._2, false)


    //  val stream = KafkaUtils.createDirectStream[String, String](
    //    ssc,
    //    PreferConsistent,
    //    Subscribe[String, String](topics, kafkaParams)
    //  )

    val inputDStream = KafkaUtils.createDirectStream(ssc, PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topics, kafkaParams, readOffsets(topics,kafkaParams.apply("group.id").toString,zkUtils)))

    //inputDStream.map(record => (record.key, record.value))

    inputDStream.foreachRDD((rdd,batchTime) => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(offset => println(offset.topic, offset.partition, offset.fromOffset,offset.untilOffset))
      //val newRDD = rdd.map(message => processMessage(message))
      //newRDD.count()
      //saveOffsets(topic,consumerGroupID,offsetRanges,hbaseTableName,batchTime) //save the offsets to HBase
      persistOffsets(offsetRanges.toSeq,kafkaParams.apply("group.id").toString,true,zkUtils)
    })
    val clickHashKey = "app::users::click"
    val userClicks =inputDStream.flatMap(line => {
      val data = JSONObject.fromObject(line.value())
      Some(data)
    }).map(x => (x.getString("uid"), x.getInt("click_count"))).reduceByKey(_ + _)
    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(pair => {

          /**
            * Internal Redis client for managing Redis connection {@link Jedis} based on {@link RedisPool}
            */
          object InternalRedisClient extends Serializable {

            @transient private var pool: JedisPool = null

            def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
                         maxTotal: Int, maxIdle: Int, minIdle: Int): Unit = {
              makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle, true, false, 10000)
            }

            def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
                         maxTotal: Int, maxIdle: Int, minIdle: Int, testOnBorrow: Boolean,
                         testOnReturn: Boolean, maxWaitMillis: Long): Unit = {
              if(pool == null) {
                val poolConfig = new GenericObjectPoolConfig()
                poolConfig.setMaxTotal(maxTotal)
                poolConfig.setMaxIdle(maxIdle)
                poolConfig.setMinIdle(minIdle)
                poolConfig.setTestOnBorrow(testOnBorrow)
                poolConfig.setTestOnReturn(testOnReturn)
                poolConfig.setMaxWaitMillis(maxWaitMillis)
                pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)

                val hook = new Thread{
                  override def run = pool.destroy()
                }
                sys.addShutdownHook(hook.run)
              }
            }

            def getPool: JedisPool = {
              assert(pool != null)
              pool
            }
          }

          // Redis configurations
          val maxTotal = 10
          val maxIdle = 10
          val minIdle = 1
          val redisHost = "slave4"
          val redisPort = 6379
          val redisTimeout = 30000
          val dbIndex = 1
          InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)

          val uid = pair._1
          val clickCount = pair._2
          val jedis =InternalRedisClient.getPool.getResource
          //println(jedis.)
          //jedis.select(dbIndex)
          //println(uid+"---"+clickCount)
          //redis 查看數據 redis-cli -h slave2  -p 7008  再使用HGETALL app::users::click
          jedis.hincrBy(clickHashKey, uid, clickCount)
          InternalRedisClient.getPool.returnResource(jedis)
        })
      })
    })
    userClicks.print()
    ssc.start()
    ssc.awaitTermination()
  }


}  