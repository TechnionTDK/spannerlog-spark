package spannerlog.app_04_splitter_framework

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import spannerlog.Utils.{isRunningOnLocal, log}
import spannerlog.nlp.Span
import spannerlog.{Runner, SparkSessionApp}

import scala.util.matching.Regex

object TriGramExtractor extends SparkSessionApp {
  var title: String = _
  var split: Boolean = _
  var datasetPath: String = _
  var isOneSentence:Boolean = _

  def setExperiment(test: String, split: String, isOneSentence: Boolean): Unit = {
    this.title = s"04-splitter-framework/$test"
    this.split = split.equals("split")
    this.datasetPath =
      (if (isRunningOnLocal) s"src/main/resources/wikipedia/$split/"
      else Runner.hdfsUrl + s"thesis/benchmark/05-wikipedia/$test/$split/app-spark/") + "wiki-articles.csv"
    this.isOneSentence = isOneSentence

    log.append(s"title: $title\n")
    log.append(s"split: ${this.split}\n")
    log.append(s"isOneSentence: $isOneSentence\n")
    log.append(s"Input: $datasetPath\n\n")

    println("Experiment is Set")

  }

  override def preprocess(ss: SparkSession): Unit = {
    assert(datasetPath != null)

    var wikipediaSchemaArray = Array(StructField("text", StringType, nullable = false))
//    if (split)
//      wikipediaSchemaArray = wikipediaSchemaArray.patch(9, List(StructField("sentence_no", StringType, nullable = false)), 0)

    import ss.implicits._
    val articles: Dataset[String] = ss.read
      .option("header", "true")
      .schema(StructType(wikipediaSchemaArray))
      .text(datasetPath)
      .as[String]
    //    reviews.show()

//    log.append("count:" + articles.count() + "\n")

    articles.cache()
    articles.createOrReplaceTempView("articles")

    val pattern = new Regex("""\w+ \w+ \w+""", "x")
    ss.udf.register("RGX", (text: String) => pattern.findAllMatchIn(text).map(m => Span(m.start(0)+1, m.end(0)+1)).toList)
  }

  override def experiment(ss: SparkSession): Unit = {
    val results = ss.sql("" +
      "SELECT explode(RGX(text)) " +
      "FROM articles ")

    val collected: Array[Row] = results.collect()

    log append s"results set size: ${collected.length}\n"
    log append(collected.take(10).mkString("\n") + "\n")


//    log append s"results set size: ${collected.length}\n"
  }
}

