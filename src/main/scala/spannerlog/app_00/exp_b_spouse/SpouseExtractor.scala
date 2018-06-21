package spannerlog.app_00.exp_b_spouse

import java.util

import edu.stanford.nlp.ie.util.RelationTriple
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.util.{CoreMap, PropertiesUtils}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spannerlog.SparkContextApp
import spannerlog.Utils.{isRunningOnLocal, timeit}
import spannerlog.data.{SignalMediaArticle, SignalMediaData}

import scala.collection.JavaConversions._

object SpouseExtractor extends SparkContextApp {

  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(
    PropertiesUtils.asProperties(
      "annotators", "tokenize,ssplit,pos,depparse,lemma,ner,mention,coref,regexner,kbp",
      "ssplit.isOneSentence", "false",
      "tokenize.language", "en"))

  def process(text: String): List[String] = {
    val document = new Annotation(text) // create an empty Annotation just with the given text
    pipeline.annotate(document) // run all Annotators on this text

    // these are all the sentences in this document
    // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
    val sentences: List[CoreMap] = document.get(classOf[SentencesAnnotation]).toList

//    (for {
//      sentence: CoreMap <- sentences
//      relation = sentence.get(classOf[MachineReadingAnnotations.RelationMentionsAnnotation])
//
//    } yield (sentence, relation)) foreach(
//      t => println("sentence:\n" + t._1 +
//        "\n\n Relation:\n" + t._2 +
//        "\n\n"))

    for (s <- sentences) {
      val m: util.List[RelationTriple] = s.get(classOf[CoreAnnotations.KBPTriplesAnnotation])
      for (r <- m) {
        println(r.toString, "gloss:", r.relationGloss())
      }
    }

    List()

//    sentences.flatMap(s => s.get())
//      .foreach(r => println(r.toString))
//
    sentences
      .flatMap(s => s.get(classOf[CoreAnnotations.KBPTriplesAnnotation]))
      .filter(triple => triple.relationGloss() == "per:spouse")
      .map(triple => (triple.subjectGloss(), triple.objectGloss()).toString())
  }

  def extractNaive(newsRdd: RDD[SignalMediaArticle]): Array[String] = {
    newsRdd.flatMap(n => process(n.content)).take(5)
  }

  /* Experiment: Extracting spouse mentions from news articles */
  def experiment(sc: SparkContext): Unit = {
    val newsRdd: RDD[SignalMediaArticle] =
      if (isRunningOnLocal) sc.textFile(SignalMediaData.filePath).map(SignalMediaData.parse).cache()
      else sc.textFile("hdfs://tdkynsparkmaster:54310/user/yoavn/sample-1H.jsonl").map(SignalMediaData.parse).cache()

    val spousesNaive: Array[String] = timeit("experiment 2a: naive spouse extraction", SpouseExtractor.extractNaive(newsRdd))

    println(spousesNaive.length)
    println(spousesNaive(0))
    println(spousesNaive(1))
  }
}
