package spannerlog.nlp

trait TextAnnotator {
  def ner(text: String): List[RecognizedEntity]
  def sentence(text: String): List[Span]
  def sentiment(text: String): List[SentimentAnnotation]
}

case class Span(begin: Int, end: Int)
case class RecognizedEntity(location: Span, category: String)
case class SentimentAnnotation(location: Span, sentimentLabel: String, sentimentValue: Int)
