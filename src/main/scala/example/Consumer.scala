import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.log4j.{Level, Logger}

object SparkKafkaConsumer {
  def main(args: Array[String]): Unit = {
    val kafkaServer = "localhost:9092"
    val topic = "logs"
    val groupId = "log-analyzer-group"

    val sparkConf = new SparkConf().setAppName("SparkKafkaConsumer").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Set log level to warn to reduce verbosity
    Logger.getRootLogger.setLevel(Level.WARN)

    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaServer,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )

    val topics = Array(topic)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    // Accumulateurs pour stocker les erreurs, avertissements et logs
    val errorCountAccumulator = ssc.sparkContext.longAccumulator("ErrorCount")
    val warnCountAccumulator = ssc.sparkContext.longAccumulator("WarnCount")
    val logCountAccumulator = ssc.sparkContext.longAccumulator("LogCount")

    stream.foreachRDD { rdd =>
      rdd.foreach { record =>
        val logLine = record.value()
        println(s"Received log: $logLine") // Debug: print each received log

        val logState = analyzeLog(logLine, errorCountAccumulator, warnCountAccumulator, logCountAccumulator)
        if (logState.nonEmpty) {
          println(s"$logState\nLog: $logLine")
        }
      }
      // Debug: Print counts after each batch
      println("Current counts:")
      printErrorCountTable(errorCountAccumulator, warnCountAccumulator, logCountAccumulator)
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def analyzeLog(input: String, errorCountAccumulator: org.apache.spark.util.LongAccumulator,
                 warnCountAccumulator: org.apache.spark.util.LongAccumulator,
                 logCountAccumulator: org.apache.spark.util.LongAccumulator): String = {
    // Check if the log contains the "error" or "warn" pattern
    logCountAccumulator.add(1) // Increment the log count

    if (input.toLowerCase.contains("error")) {
      errorCountAccumulator.add(1) // Increment the error count
      "\u001b[31mERROR DETECTED\u001b[0m" // ANSI escape code for red color
    } else if (input.toLowerCase.contains("warn")) {
      warnCountAccumulator.add(1) // Increment the warning count
      ""
    } else {
      ""
    }
  }

  def printErrorCountTable(errorCountAccumulator: org.apache.spark.util.LongAccumulator,
                           warnCountAccumulator: org.apache.spark.util.LongAccumulator,
                           logCountAccumulator: org.apache.spark.util.LongAccumulator): Unit = {
    println("Error, Warning, and Log Count:")
    println("+--------------+-----------+-----------+")
    println("|   Log Type   |  Errors   | Warnings  | Logs  |")
    println("+--------------+-----------+-----------+")
    println(f"|   Count      |   ${errorCountAccumulator.value}%-9d|   ${warnCountAccumulator.value}%-9d|   ${logCountAccumulator.value}%-9d|")
    println("+--------------+-----------+-----------+")
    println()
  }
}
