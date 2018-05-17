import java.util
import java.util.{Arrays, HashSet, Set}

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import redis.clients.jedis.JedisPool
import net.sf.json.JSONObject


object KafkaStreamingTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-common-2.2.0-bin-master")
    var masterUrl = "local[1]"
    if (args.length > 0) {
      masterUrl = args(0)
    }
    val brokers = "192.168.81.220:9092,192.168.81.221:9092,192.168.81.223:9092"
    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("KafkaStreamingTest")
    //val sparkContext = new SparkContext(conf)
    val streamingContext = new StreamingContext(conf, Seconds(5))
    val kafkaParams = Map[String, Object](
     "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "click",
      "client.id" ->"test1",
      "consumer.id" -> "c1",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)

    )


    val topics = Array("user_events_0.10")
    /*val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      ConsumerStrategies.Subscribe(topics, kafkaParams)
    )
    stream.map(record => (record.key, record.value))*/
    val offsetRanges = Array(
      // topic, partition, inclusive starting offset, exclusive ending offset
      OffsetRange("test", 0, 0, 100),
      OffsetRange("test", 1, 0, 100)
    )

   // val rdd = KafkaUtils.createRDD[String, String](sparkContext, kafkaParams, offsetRanges, PreferConsistent)

    /*val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
    )*/
    val clickHashKey = "app::users::click"
    val topic : String = "user_events_0.10"  //消费的 topic 名字
    val topicDirs = new ZKGroupTopicDirs("test_spark_streaming_group_0.10", topic)  //创建一个 ZKGroupTopicDirs 对象，对保存
    val zkClient = new ZkClient("master:2181,slave1:2181,slave2:2181")          //zookeeper 的host 和 ip，创建一个 client
    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")    //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
    val topiciterator=Array(new TopicPartition(topic,0))
    //此处把1个或多个topic解析为集合对象，因为后面接口需要传入Collection类型
    var  kafkaStream:InputDStream[ConsumerRecord[String, String]]=null
   // var kafkaStream : InputDStream[(String, String)] = null
    var fromOffsets: Map[TopicPartition, Long] = Map()  //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置

    if (children > 0) {  //如果保存过 offset，这里更好的做法，还应该和  kafka 上最小的 offset 做对比，不然会报 OutOfRange 的错误
      for (i <- 0 until children) {
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        val tp = new TopicPartition(topic, i)
        fromOffsets += (tp -> partitionOffset.toLong)  //将不同 partition 对应的 offset 增加到 fromOffsets 中
        //logInfo("@@@@@@ topic[" + topic + "] partition[" + i + "] offset[" + partitionOffset + "] @@@@@@")
      }


      kafkaStream = KafkaUtils.createDirectStream[String, String](streamingContext,PreferConsistent, ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams,fromOffsets))
    }
    else {
      /*kafkaStream = KafkaUtils.createDirectStream[String, String](
        streamingContext,
        PreferConsistent,
        Subscribe(topics.toList, kafkaParams))*/

      kafkaStream = KafkaUtils.createDirectStream[String, String](streamingContext,PreferConsistent, ConsumerStrategies.Assign[String, String](topiciterator.toList, kafkaParams,fromOffsets))
     // val kafkaStream = KafkaUtils.createDirectStream[String, String](streamingContext,PreferConsistent, ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)) //如果未保存，根据 kafkaParam 的配置使用最新或者最旧的 offset
    }
    //var offsetRanges = Array[OffsetRange]()
    topics.map(_.toString).toSet
   /* val kafkaStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      ConsumerStrategies.Subscribe(topics, kafkaParams)
    )*/
    //val kafkaStream = KafkaUtils.createDirectStream[String, String](streamingContext,PreferConsistent, ConsumerStrategies.Assign[String, String](topiciterator.toList, kafkaParams,fromOffsets))
    kafkaStream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该 rdd 对应 kafka 的消息的 offset
      for (o <- offsetRanges) {
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        if(zkClient.exists(zkPath))
          zkClient.writeData(zkPath, o.fromOffset.toString)
        else {
          import org.apache.zookeeper.CreateMode
          //zkClient.create(zkPath, o.fromOffset.toString, CreateMode.PERSISTENT)
          zkClient.createPersistent(zkPath, o.fromOffset.toString)
        }

        //ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString)  //将该 partition 的 offset 保存到 zookeeper
        //logInfo(s"@@@@@@ topic  ${o.topic}  partition ${o.partition}  fromoffset ${o.fromOffset}  untiloffset ${o.untilOffset} #######")
      }

    }

    val userClicks =kafkaStream.flatMap(line => {
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
          val redisHost = "slave2"
          val redisPort = 7008
          val redisTimeout = 30000
          val dbIndex = 1
          InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)

          val uid = pair._1
          val clickCount = pair._2
          val jedis =InternalRedisClient.getPool.getResource
          //println(jedis.)
          //jedis.select(dbIndex)
          //println(uid+"---"+clickCount)
          jedis.hincrBy(clickHashKey, uid, clickCount)
          InternalRedisClient.getPool.returnResource(jedis)
        })
      })
    })
    userClicks.print()
    streamingContext.start()
    streamingContext.awaitTermination()


  }

}
