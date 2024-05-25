import java.io.File
import scala.io.Source
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Producer {
  def main(args: Array[String]): Unit = {
    val kafkaServer = "localhost:9092"
    val topic = "logs"

    val props = new java.util.Properties()
    props.put("bootstrap.servers", kafkaServer)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val filePath = "C:/Users/sawssen/Desktop/Spark project/logsGenerator/logs.txt"

    val file = new File(filePath)

    // Fonction pour envoyer les logs à Kafka
    def sendLogToKafka(log: String): Unit = {
      try {
        val record = new ProducerRecord[String, String](topic, null, log)
        producer.send(record)
      } catch {
        case e: Exception =>
          println(s"Erreur lors de l'envoi du log à Kafka : ${e.getMessage}")
      }
    }

    // Fonction pour surveiller les nouveaux logs dans le fichier
    def monitorFileForLogs(file: File): Unit = {
      var lastReadPosition = 0L // Mémoriser la dernière position lue dans le fichier
      
      while (true) {
        val bufferedSource = Source.fromFile(file)
        bufferedSource.getLines().drop(lastReadPosition.toInt).foreach { line =>
          sendLogToKafka(line)
          lastReadPosition += 1 // Mettre à jour la dernière position lue
        }
        bufferedSource.close()
        
        Thread.sleep(1000) // Attendre 1 seconde avant de vérifier le fichier à nouveau
      }
    }

    // Gestion de l'interruption
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      println("Arrêt du producteur...")
      producer.close()
    }))

    // Démarrer la surveillance du fichier
    monitorFileForLogs(file)
  }
}
