package spannerlog.app_00.exp_a_persons

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.util.PropertiesUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spannerlog.SparkContextApp
import spannerlog.Utils.{isRunningOnLocal, timeit}
import spannerlog.data.{WikipediaArticle, WikipediaData}

import scala.collection.JavaConversions._

object PersonsRecognizer extends SparkContextApp {

  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(
    PropertiesUtils.asProperties(
      "annotators", "tokenize, ssplit, pos, lemma, ner",
      "ssplit.isOneSentence", "false",
      "tokenize.language", "en"))

  def process(text: String): List[String] = {
    val document = new Annotation(text) // create an empty Annotation just with the given text
    pipeline.annotate(document) // run all Annotators on this text

    // these are all the sentences in this document
    // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
    val sentences = document.get(classOf[CoreAnnotations.SentencesAnnotation])

    sentences
      .flatMap(s => s.get(classOf[CoreAnnotations.TokensAnnotation]))
      .map(t => (t, t.get(classOf[CoreAnnotations.NamedEntityTagAnnotation])))
      .filter(p => p._2 == "PERSON")
      .map(p => p._1.get(classOf[CoreAnnotations.TextAnnotation]))
      .toList
  }

  def calcAverageWords(rdd: RDD[WikipediaArticle]): Float =
    rdd.map(w => w.text.split(' ').length).reduce(_ + _) / rdd.count().asInstanceOf[Float]

  def extractPersons1(rdd: RDD[WikipediaArticle]): Array[(Int, List[String])] =
    rdd.map(w => (w.id, process(w.text))).collect()

  def extractPersons2(rdd: RDD[WikipediaArticle]): Long =
    rdd.flatMap(w => process(w.text).map(s => (w.id, s))).count()

  def extractPersonsSplitRegex1(rdd: RDD[WikipediaArticle]): Array[(Int, List[String])] = {
    rdd.flatMap(w => w.text.split('.').filter(s => s.nonEmpty).map(s => (w.id, s)))
      .map(p => (p._1, process(p._2)))
      .collect()
  }

  def extractPersonsSplitRegex2(rdd: RDD[WikipediaArticle]): Long = {
    rdd.flatMap(w => w.text.split('.').filter(s => s.nonEmpty).map(s => (w.id, s)))
      .flatMap(p => process(p._2).map(s => (p._1, s)))
      .count()
  }

  /* Experiment: Extracting person mentions */
  def experiment(sc: SparkContext): Unit = {
    val wikiRdd: RDD[WikipediaArticle] =
      if (isRunningOnLocal) sc.textFile(WikipediaData.filePath).map(WikipediaData.parse).cache()
      else sc.textFile("hdfs://tdkynsparkmaster:54310/user/yoavn/wikipedia-1H.dat").map(WikipediaData.parse).cache()

    //    println(wikiRdd.map(w => w.text.split('.').length).sum() / wikiRdd.count())

    val personMentionsNaive1: Array[(Int, List[String])] = timeit("1a: naive person mentions extraction (collect)", PersonsRecognizer.extractPersons1(wikiRdd))
    val personMentionsNaive2: Long = timeit("1b: naive person mentions extraction (count)", PersonsRecognizer.extractPersons2(wikiRdd)) // 763810 ms. Returned value: 211245

    val personMentionsRegex1: Array[(Int, List[String])] = timeit("experiment 1c: persons recognition with regex splitter", PersonsRecognizer.extractPersonsSplitRegex1(wikiRdd)) // 810091 ms
    val personMentionsRegex2: Long = timeit("experiment 1d: persons recognition with regex splitter", PersonsRecognizer.extractPersonsSplitRegex2(wikiRdd)) // 796604 ms. Returned value: 22104
  }
}
