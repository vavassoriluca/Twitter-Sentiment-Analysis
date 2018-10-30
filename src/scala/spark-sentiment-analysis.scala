package sparkstreaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}

object KafkaSpark {

    def main(args: Array[String]) {
        
        // make a connection to Kafka and read (key, value) pairs from it
        val conf = new SparkConf().setAppName("twitter-sentiment-analysis").setMaster("local[3]")
        val sc = SparkContext.getOrCreate(conf)
        val ssc = new StreamingContext(sc, Seconds(1))    
        ssc.checkpoint("./checkpoint")

        val kafkaConf = Map(
            "metadata.broker.list" -> "localhost:9092",
            "zookeeper.connect" -> "localhost:2181",
            "group.id" -> "kafka-spark-streaming",
            "zookeeper.connection.timeout.ms" -> "1000")

        val topics = Set("twitter")

        val tweets = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topics)

        tweets.print()       

        ssc.start()
        ssc.awaitTermination()
    
    }

}