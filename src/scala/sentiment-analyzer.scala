package sparkstreaming

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

import java.util.{Date, Properties}
import scala.collection.JavaConversions._

// ******** SENTIMENT OBJECT ********

object Sentiment extends Enumeration {
    type Sentiment = Value
    val POSITIVE, NEGATIVE, NEUTRAL = Value

    def toSentiment(sentiment: Int): Sentiment = sentiment match {
        case x if x >= 0 || x < 1.5 => Sentiment.NEGATIVE
        case x if x >= 1.5 || x < 2.5 => Sentiment.NEUTRAL
        case x if x >= 2.5 => Sentiment.POSITIVE
    }
}

// ******** SENTIMENT ANALYZER ********

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
 
    def extractSentiment(text: String) : Float = {
        val finalSentiment = SentimentAnalyzer.sentimentAvg(SentimentAnalyzer.extractSentiments(text))
        if (finalSentiment.isNaN || finalSentiment.isInfinity)
            return -1.toFloat
        else
            return finalSentiment
    }

}