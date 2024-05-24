import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.Properties
import scala.collection.JavaConverters._

import scala.collection.mutable.Map

object Consumer {
  def main(args: Array[String]): Unit = {
    val kafkaServer = "localhost:9092"
    val topic = "logs" // Same topic as used by the producer

    // Kafka consumer configuration
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "log-analyzer-group")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // Start from the beginning of the topic

    // Set log level to "warn"
    import org.apache.log4j.{Level, Logger}
    Logger.getRootLogger.setLevel(Level.WARN)

    val consumer = new KafkaConsumer[String, String](props)

    // Subscribe to the topic
    consumer.subscribe(List(topic).asJava)

    // Mutable maps to store error, warning, and log counts
    val errorCountMap = Map[String, Int]().withDefaultValue(0)
    val warnCountMap = Map[String, Int]().withDefaultValue(0)
    val logCountMap = Map[String, Int]().withDefaultValue(0)

    // Message consumption loop
    while (true) {
      val records = consumer.poll(java.time.Duration.ofMillis(100))
      for (record <- records.asScala) {
        val logLine = record.value()
        val logState = analyzeLog(logLine, errorCountMap, warnCountMap, logCountMap)
        if (logState.nonEmpty) {
          println(s"$logState\nLog: $logLine")
          if (logState.contains("ERROR DETECTED")) {
            printErrorCountTable(errorCountMap, warnCountMap, logCountMap)
          }
        }
      }
    }
  }

  // Log analysis function
  def analyzeLog(input: String, errorCountMap: scala.collection.mutable.Map[String, Int], 
                 warnCountMap: scala.collection.mutable.Map[String, Int], 
                 logCountMap: scala.collection.mutable.Map[String, Int]): String = {
    // Check if the log contains the "error" pattern
    logCountMap("log") += 1 // Increment the log count

    if (input.toLowerCase.contains("error")) {
      errorCountMap("error") += 1 // Increment the error count
      "\u001b[31mERROR DETECTED\u001b[0m" // ANSI escape code for red color
    } else if (input.toLowerCase.contains("warn")) {
      warnCountMap("warn") += 1 // Increment the warning count
      ""
    } else {
      ""
    }
  }

  // Function to print error, warning, and log count table
  def printErrorCountTable(errorCountMap: scala.collection.mutable.Map[String, Int], 
                            warnCountMap: scala.collection.mutable.Map[String, Int], 
                            logCountMap: scala.collection.mutable.Map[String, Int]): Unit = {
    println("Error, Warning, and Log Count:")
    println("+--------------+-----------+------------------+")
    println("|   Log Type   | Errors    | Warnings | Logs  |")
    println("+--------------+-----------+------------------+")
    println(s"|   Count      |   ${errorCountMap("error")}${" " * (8 - errorCountMap("error").toString.length)}|   ${warnCountMap("warn")}${" " * (7 - warnCountMap("warn").toString.length)}|   ${logCountMap("log")}${" " * (4 - logCountMap("log").toString.length)}|")
    println("+--------------+-----------+------------------+")
    println()
  }
}
