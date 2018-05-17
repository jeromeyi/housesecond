import java.util.Properties

import kafka.producer.{KeyedMessage, ProducerConfig}
import net.sf.json.JSONObject

import scala.util.Random

object KafkaEventProducer {
  private val users = Array(
    "4A4D769EB9679C054DE81B973ED5D768", "8dfeb5aaafc027d89349ac9a20b3930f",
    "011BBF43B89BFBF266C865DF0397AA71", "f2a8474bf7bd94f0aabbd4cdd2c06dcf",
    "068b746ed4620d25e26055a9f804385f", "97edfc08311c70143401745a03a50706",
    "d7f141563005d1b5d0d3dd30138f3f62", "c8ee90aade1671a21336c721512b817a",
    "6b67c8c700427dee7552f81f3228c927", "a95f22eabc4fd4b580c011a3161a9d9d")

  private val random = new Random()

  private var pointer = -1

  def getUserID() : String = {
    pointer = pointer + 1
    if(pointer >= users.length) {
      pointer = 0
      users(pointer)
    } else {
      users(pointer)
    }
  }

  def click() : Double = {
    random.nextInt(10)
  }

  // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --create --topic user_events --replication-factor 2 --partitions 2
  // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --list
  // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --describe user_events
  // bin/kafka-console-consumer.sh --zookeeper zk1:2181,zk2:2181,zk3:22181/kafka --topic test_json_basis_event --from-beginning
  //kafka-topics.sh --zookeeper master:2181,slave1:2181,slave2:2181 --create --topic user_events --replication-factor 2 --partitions 2
  //kafka-topics.sh --zookeeper master:2181,slave1:2181,slave2:2181,slave3:2181,slave4:2181 --create --topic user_events_10 --replication-factor 3 --partitions 10
  //kafka-topics.sh --describe --zookeeper master:2181,slave1:2181,slave2:2181,slave3:2181,slave4:2181 --topic user_events_10
  def main(args: Array[String]): Unit = {
    //val topic = "user_events"
    val topic = "user_events_10"
    //val brokers = "master:9092,slave1:9092,slave2:9092,slave3:9092,slave4:9092"
    val brokers = "172.16.11.51:9092,172.16.11.52:9092,172.16.11.53:9092,172.16.11.54:9092,172.16.11.55:9092"
    val props = new Properties()
    //props.put("metadata.broker.list", brokers)
   // props.put("serializer.class", "kafka.serializer.StringEncoder")
    //props.put("request.required.acks", "1")
    //props.put("producer.type", "async")


    import org.apache.kafka.clients.producer.KafkaProducer
    import org.apache.kafka.clients.producer.Producer

    props.put("bootstrap.servers", brokers)
    //props.put("acks", "all")
    //props.put("retries", 0)
    //props.put("batch.size", 16384)
    //props.put("linger.ms", 1)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    while(true) {
      // prepare event data
     /* val event = new JSONObject()
      event
        .put("uid", getUserID)
        .put("event_time", System.currentTimeMillis.toString)
        .put("os_type", "Android")
        .put("click_count", click)*/

      val event = new JSONObject()
      event.put("uid", getUserID)
      event.put("event_time", System.currentTimeMillis.toString)
      event.put("os_type", "Android")
      event.put("click_count", click)
      import org.apache.kafka.clients.producer.ProducerRecord
      val r = new ProducerRecord[String, String](topic, event.toString )
      producer.send(r);
      println("Message send: " + event)

      Thread.sleep(200)
    }
  }
}
