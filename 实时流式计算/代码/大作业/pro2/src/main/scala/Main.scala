import com.alibaba.fastjson.JSON
import com.bingocloud.{ClientConfiguration, Protocol}
import com.bingocloud.auth.BasicAWSCredentials
import com.bingocloud.services.s3.AmazonS3Client
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.nlpcn.commons.lang.util.IOUtil
import java.util.{Properties, UUID}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object Main {
  //s3参数
  val accessKey = "FEA16476EE52A96FD9B6"
  val secretKey = "WzFENDNBM0U1RjZGNTAwQzQ1N0VGQURBM0UzOEZC"
  val endpoint = "http://scut.depts.bingosoft.net:29997"
  val bucket = "zhu"
  //要读取的文件
  val key = "demo.txt"
  //上传文件的路径前缀
  val keyPrefix = "upload/"
  //上传数据间隔 单位毫秒
  val period = 5000
  //kafka参数
  val topic = "my_topic_1"
  val bootstrapServers = "bigdata35.depts.bingosoft.net:29035,bigdata36.depts.bingosoft.net:29036,bigdata37.depts.bingosoft.net:29037"

  def main(args: Array[String]): Unit = {
//    val s3Content = readFile()
//    produceToKafka(s3Content)
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(56)
//    val kafkaProperties = new Properties()
//    kafkaProperties.put("bootstrap.servers", bootstrapServers)
//    kafkaProperties.put("group.id", UUID.randomUUID().toString)
//    kafkaProperties.put("auto.offset.reset", "earliest")
//    kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    val kafkaConsumer = new FlinkKafkaConsumer010[String](topic,
//      new SimpleStringSchema, kafkaProperties)
//    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
//    val inputKafkaStream = env.addSource(kafkaConsumer)
//    val stream1 = inputKafkaStream.map(x => JSON.parseObject(x))
//    val stream2 = stream1.map(x => BuyTicket(x.getString("buy_time"),x.getString("buy_address"),x.getString("origin"),x.getString("destination"),x.getString("username")))
//    stream2.keyBy("destination").print()
//    stream2.keyBy("destination").writeAsText("D:\\download")
//    env.execute()

    print("写入s3")
    new Writer()
  }
  /**
   * 从s3中读取文件内容
   *
   * @return s3的文件内容
   */
  def readFile(): String = {
    val credentials = new BasicAWSCredentials(accessKey, secretKey)
    val clientConfig = new ClientConfiguration()
    clientConfig.setProtocol(Protocol.HTTP)
    val amazonS3 = new AmazonS3Client(credentials, clientConfig)
    amazonS3.setEndpoint(endpoint)
    val s3Object = amazonS3.getObject(bucket, key)
    IOUtil.getContent(s3Object.getObjectContent, "UTF-8")
  }

  /**
   * 把数据写入到kafka中
   *
   * @param s3Content 要写入的内容
   */
  def produceToKafka(s3Content: String): Unit = {
    val props = new Properties
    props.put("bootstrap.servers", bootstrapServers)
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val dataArr = s3Content.split("\n")
    for (s <- dataArr) {
      if (!s.trim.isEmpty) {
        val record = new ProducerRecord[String, String](topic, null, s)
        println("开始生产数据：" + s)
        producer.send(record)
      }
    }
    producer.flush()
    producer.close()
  }
}