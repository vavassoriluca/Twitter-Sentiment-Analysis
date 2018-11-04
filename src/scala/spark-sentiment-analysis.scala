package sparkstreaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import scala.collection.convert.wrapAll._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}

import scalaj.http._

import sparkstreaming._


object KafkaSpark {

    def main(args: Array[String]) {

        // ******** KAFKA CONSUMER ********

        // make a connection to Kafka 
        val conf = new SparkConf().setAppName("twitter-sentiment-analysis").setMaster("local[3]")
        val sc = SparkContext.getOrCreate(conf)
        sc.setLogLevel("WARN")
        val ssc = new StreamingContext(sc, Seconds(1))    
        ssc.checkpoint("./checkpoint")

        val kafkaConf = Map(
            "metadata.broker.list" -> "localhost:9092",
            "zookeeper.connect" -> "localhost:2181",
            "group.id" -> "kafka-spark-streaming",
            "zookeeper.connection.timeout.ms" -> "1000")

        val topics = Set("twitter")

        // DirectSream from Kafka
        val tweets = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topics)

        //Function to covert the State to a Json string
        def stateToJson(state: (Long, Long, Long, Long, Float)): String = {
            val json = s"{${'"'}tweets${'"'}: ${state._1}, ${'"'}negative${'"'}: ${state._2}, ${'"'}neutral${'"'}: ${state._3},${'"'}positive${'"'}: ${state._4}, ${'"'}avg${'"'}: ${state._5}}"
            return json
        }

        // Receive as Input a list of Lines of a tweet along with the related sentiment
        // The Output consists of a Json representation of the new state
        // The State is a tuple that consists of number of tweets precessed, number of negative, neutral and positive tweets, sentiment average
        def stateUpdateFunction(key: String, value: Option[Float], state: State[(Long, Long, Long, Long, Float)]): String = {
            
            var oldState: (Long, Long, Long, Long, Float) = state.getOption.getOrElse[(Long, Long, Long, Long, Float)]((0.toLong, 0.toLong, 0.toLong, 0.toLong, 0.0.toFloat))
            val sum = oldState._1 * oldState._5
            var newCount = oldState._1 + 1
            var count0 = oldState._2
            var count1 = oldState._3
            var count2 = oldState._4
            if (value.isEmpty || value.get == -1) {
                state.update(oldState)
                return stateToJson(oldState)
            }
            else  {
                value.get match {
                    case x if x >= 0 && x < 1.6 => count0 += 1
                    case x if x >= 1.6 && x < 3 => count1 += 1
                    case x if x >= 3 => count2 += 1
                }
                val newState: (Long, Long, Long, Long, Float) = (newCount, count0, count1, count2, (sum + value.get) / newCount)
                state.update(newState)
                //println(newState)
                return stateToJson(newState)
            }
            
        }

        // ******** SPARK STREAMING JOB ********

        val sentiments = tweets.map{tweet => (tweet._1, SentimentAnalyzer.extractSentiment(tweet._2))}
            .mapWithState(StateSpec.function(stateUpdateFunction _))
            .foreachRDD { rdd =>
                rdd.foreachPartition { partitionOfRecords =>
                    partitionOfRecords.foreach{record => 
                        val request: HttpRequest = Http("http://localhost:4567/payload").postData(record)
                        println(record)
                        request.execute()
                    }
                }
            }

        ssc.start()
        ssc.awaitTermination()
    }

}