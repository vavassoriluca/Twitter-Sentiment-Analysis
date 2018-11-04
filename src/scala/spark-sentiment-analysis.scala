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
                var finalValue: Float = 2.toFloat
                value.get match {
                    case x if x >= 0 && x < 1.5 => {count0 += 1; finalValue = 0.75.toFloat}
                    case x if x >= 1.5 && x < 2.5 => {count1 += 1; finalValue = 2.toFloat}
                    case x if x >= 2.5 => {count2 += 1; finalValue = 3.25.toFloat}
                }
                val newState: (Long, Long, Long, Long, Float) = (newCount, count0, count1, count2, (sum + finalValue) / newCount)
                state.update(newState)
                //println(newState)
                return stateToJson(newState)
            }
            
        }

        // ******** SPARK STREAMING JOB ********

        val sentiments = tweets.map{tweet => 
                val key = tweet._2.split("@B1A2R3R4I5E6R7@")(0)
                val value = tweet._2.split("@B1A2R3R4I5E6R7@")(1)
                (key, SentimentAnalyzer.extractSentiment(value))
            }
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