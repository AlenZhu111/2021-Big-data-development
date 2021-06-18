import java.sql.DriverManager
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


object Main {
  //kafka参数
  val topic = "demo_2"
  val bootstrapServers = "bigdata35.depts.bingosoft.net:29035,bigdata36.depts.bingosoft.net:29036,bigdata37.depts.bingosoft.net:29037"

  def main(args: Array[String]): Unit = {
    val s3Content = readFile()
    produceToKafka(s3Content)
  }

  /**
   * 从s3中读取文件内容
   *
   * @return s3的文件内容
   */
  def readFile(): String = {
    val url = "jdbc:hive2://bigdata115.depts.bingosoft.net:22115/user18_db"
    val properties = new Properties()
    properties.setProperty("driverClassName", "org.apache.hive.jdbc.HiveDriver")
    properties.setProperty("user", "user18")
    properties.setProperty("password", "pass@bingo18")

    val connection = DriverManager.getConnection(url, properties)

    val statement = connection.createStatement
    var res = ""
    val resultSet = statement.executeQuery("select * from test")
    try {
      while (resultSet.next) {
        val sfzhm = resultSet.getString(1)
        val xm = resultSet.getString(2)
        val asjbh = resultSet.getString(3)
        val ajmc = resultSet.getString(4)
        val aj_jyqk = resultSet.getString(5)
        res+=sfzhm+","+xm+","+asjbh+","+ajmc+","+aj_jyqk+"\n"
      }

    }catch {
      case e: Exception => e.printStackTrace()
    }
    res
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