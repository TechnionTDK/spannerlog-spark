package spannerlog.nlp

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.util.{CoreMap, PropertiesUtils}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


class CoreNlp(val dependencies: String, val isOneSentence: Boolean) extends TextAnnotator {
  @transient private var _pipeline: StanfordCoreNLP = _

  def getOrCreatePipeline(): StanfordCoreNLP = {
    if (_pipeline == null) {
      _pipeline = new StanfordCoreNLP(
        PropertiesUtils.asProperties(
          "annotators", dependencies,
          "ssplit.isOneSentence", isOneSentence.toString,
          "tokenize.language", "en"))
    }
    _pipeline
  }

  override def ner(text: String): List[RecognizedEntity] = {
    val pipeline = getOrCreatePipeline()
    val document = new Annotation(text) // create an empty Annotation just with the given text
    pipeline.annotate(document) // run all Annotators on this text

    // these are all the sentences in this document
    // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
    val sentences: List[CoreMap] = document.get(classOf[SentencesAnnotation]).toList

    val entities = ListBuffer[RecognizedEntity]()
    for (sentence <- sentences) {
      var begin = -1
      var end = -1
      var prevCategory = "O"
      for (token <- sentence.get(classOf[CoreAnnotations.TokensAnnotation])) { // this is the text of the token
        val category = token.get(classOf[CoreAnnotations.NamedEntityTagAnnotation])
        if (category.equals(prevCategory))
          end = token.endPosition()
        else {
          if (begin == -1) {
            begin = token.beginPosition()
            end = token.endPosition()
            prevCategory = category
          } else {
            entities += RecognizedEntity(Span(begin+1, end+1), prevCategory)
            if (!category.equals("O")) {
              begin = token.beginPosition()
              end = token.endPosition()
              prevCategory = category
            } else {
              begin = -1
              end = -1
              prevCategory = "O"
            }
          }
        }
      }
      if (begin != -1)
        entities += RecognizedEntity(Span(begin+1, end+1), prevCategory)
    }
    entities.toList
  }

  override def sentence(text: String): List[Span] = {
    val pipeline = getOrCreatePipeline()
    val document = new Annotation(text) // create an empty Annotation just with the given text
    pipeline.annotate(document) // run all Annotators on this text

    // these are all the sentences in this document
    // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
    document.get(classOf[SentencesAnnotation]).map(s => {
      val tokens = s.get(classOf[CoreAnnotations.TokensAnnotation])
      Span(tokens.head.beginPosition(), tokens.last.endPosition())
    }).toList
  }

  override def sentiment(text: String): List[SentimentAnnotation] = {
    val pipeline = getOrCreatePipeline()
    val sentimentAnnotations = ListBuffer[SentimentAnnotation]()

    val document = new Annotation(text) // create an empty Annotation just with the given text
    pipeline.annotate(document) // run all Annotators on this text

    // these are all the sentences in this document
    // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
    val sentences: List[CoreMap] = document.get(classOf[SentencesAnnotation]).toList

    for (sentence <- sentences) {
      val tokens = sentence.get(classOf[CoreAnnotations.TokensAnnotation])
      val location = Span(tokens.head.beginPosition(), tokens.last.endPosition())

      val tree = sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])

      val sentimentLabel: String = sentence.get(classOf[SentimentCoreAnnotations.SentimentClass])
      val sentimentValue: Int = RNNCoreAnnotations.getPredictedClass(tree)

      sentimentAnnotations += SentimentAnnotation(location, sentimentLabel, sentimentValue)
    }

    sentimentAnnotations.toList
  }
}
