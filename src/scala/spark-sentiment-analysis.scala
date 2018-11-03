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
import scala.collection.convert.wrapAll._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

import scalaj.http._

object Sentiment extends Enumeration {
    type Sentiment = Value
    val POSITIVE, NEGATIVE, NEUTRAL = Value

    def toSentiment(sentiment: Int): Sentiment = sentiment match {
        case x if x == 0 || x == 1 => Sentiment.NEGATIVE
        case 2 => Sentiment.NEUTRAL
        case x if x == 3 || x == 4 => Sentiment.POSITIVE
    }
}

object SentimentAnalyzer {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
    val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

    def extractSentiments(text: String): List[(String, Int)] = {
        val annotation: Annotation = pipeline.process(text)
        val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
        sentences
          .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
          .map { case (sentence, tree) => (sentence.toString,RNNCoreAnnotations.getPredictedClass(tree)) }
          .toList
    }

    def sentimentAvg(list: List[(String, Int)]) : Float = {
        var count = list.length
        var sum: Float = 0.0.toFloat
        list.foreach(t => sum += t._2.toFloat)
        return sum / count
    }

}

object KafkaSpark {
  def extractSentiments(text: String): List[(String, Sentiment.Sentiment)] = {
    val annotation: Annotation = pipeline.process(text)
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    sentences
      .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
      .map { case (sentence, tree) => (sentence.toString,Sentiment.toSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }
      .toList
  }

}


object KafkaSpark {
    def main(args: Array[String]) {
        // make a connection to Kafka and read (key, value) pairs from it
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
        val tweets = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topics)

        def stateToJson(state: (Long, Long, Long, Long, Long, Float)): String = {
            val json = s"{${'"'}tweets${'"'}: $state._1, ${'"'}negative${'"'}: $state._2, ${'"'}semi-negative${'"'}: $state._3,${'"'}semi-positive${'"'}: $state._4, ${'"'}positive${'"'}: $state._5, ${'"'}avg${'"'}: $state._6}"
            return json
        }

        // measure the average value for each key in a stateful manner
        def stateUpdateFunction(key: String, value: Option[List[(String, Int)]], state: State[(Long, Long, Long, Long, Long, Float)]): String = {
            
            var oldState: (Long, Long, Long, Long, Long, Float) = state.getOption.getOrElse[(Long, Long, Long, Long, Long, Float)]((0.toLong, 0.toLong, 0.toLong, 0.toLong, 0.toLong, 0.0.toFloat))
            val sum = oldState._1 * oldState._6
            var newCount = oldState._1 + 1
            var count0 = oldState._2
            var count1 = oldState._3
            var count2 = oldState._4
            var count3 = oldState._5
            val list = value.get
            val countSentiment = list.length
            var sumSentiment: Float = 0.0.toFloat
            list.foreach(t => sumSentiment += t._2.toFloat)
            if (value.isEmpty) {
                state.update(oldState)
                return stateToJson(oldState)
            }
            else  {
                val finalSentiment = sumSentiment / countSentiment
                finalSentiment match {
                    case x if x >= 0 && x < 1 => count0 += 1
                    case x if x >= 1 && x < 2 => count1 += 1
                    case x if x >= 2 && x < 3 => count2 += 1
                    case x if x >= 3 => count3 += 1
                }
                val newState: (Long, Long, Long, Long, Long, Float) = (newCount, count0, count1, count2, count3, (sum + finalSentiment) / newCount)
                state.update(newState)
                return stateToJson(newState)
            }
            
        }

        val sentiments = tweets.map{tweet => (tweet._1, SentimentAnalyzer.extractSentiments(tweet._2))}
            .mapWithState(StateSpec.function(stateUpdateFunction _))
            .foreachRDD { rdd =>
                rdd.foreachPartition { partitionOfRecords =>
                    val request: HttpRequest = Http("http://localhost:4567/payload")
                    partitionOfRecords.foreach(record => request.postForm(Seq("body" -> record)))
                }
            }

        ssc.start()
        ssc.awaitTermination()
    }

}