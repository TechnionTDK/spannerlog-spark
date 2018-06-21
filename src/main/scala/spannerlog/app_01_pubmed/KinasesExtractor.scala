package spannerlog.app_01_pubmed

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import spannerlog.{Runner, SparkSessionApp}
import spannerlog.Utils.{isRunningOnLocal, log}
import spannerlog.nlp.Span

import scala.util.matching.Regex

object KinasesExtractor extends SparkSessionApp {
  var title: String = _
  var split: Boolean = _
  var abstractsCsvPath: String = _
  var aliasesCsvPath: String = _

  def setExperiment(test: String, split: String): Unit = {
    this.title = s"01-pubmed/$test"
    this.split = split.equals("split")
    abstractsCsvPath = (if (isRunningOnLocal) s"src/main/resources/pubmed/$split/" else Runner.hdfsUrl + s"thesis/benchmark/01-pubmed/$test/$split/app-spark/") + "abstracts.csv"
    aliasesCsvPath = (if (isRunningOnLocal) "src/main/resources/pubmed/" else Runner.hdfsUrl + s"thesis/benchmark/01-pubmed/$test/$split/app-spark/") + "aliases.csv"

    log.append("Input:\n")
    log.append(s"  $abstractsCsvPath\n")
    log.append(s"  $aliasesCsvPath\n\n")
  }

  override def preprocess(ss: SparkSession): Unit = {
    assert (abstractsCsvPath != null)
    assert (aliasesCsvPath != null)

    var abstractsSchemaArray = Array(
      StructField("kid", IntegerType, nullable = false),
      StructField("pubmed_id", StringType, nullable = false),
      StructField("abstract", StringType, nullable = false),
      StructField("link", StringType, nullable = false),
      StructField("publication_date", DateType, nullable = false))
    if (split)
      abstractsSchemaArray = abstractsSchemaArray.patch(2, List(StructField("sentence_no", StringType, nullable = false)), 0)

    val abstracts = ss.read
      .option("header", "true")
      .schema(StructType(abstractsSchemaArray))
      .csv(abstractsCsvPath)
//    abstracts.show()
    abstracts.createOrReplaceTempView("abstracts")

    val aliasesSchema = StructType(Array(
      StructField("kid", IntegerType, nullable = false),
      StructField("alias", StringType, nullable = false)))
    val aliases = ss.read
      .option("header", "true")
      .schema(aliasesSchema)
      .csv(aliasesCsvPath)
    aliases.createOrReplaceTempView("aliases")

    val pattern1 = new Regex("""(\w+) phosphorylates[^.]*p53""", "x")
    ss.udf.register("RGX1", (text: String) => pattern1.findAllMatchIn(text).map(m => Span(m.start(1)+1, m.end(1)+1)).toList)

    val pattern2 = new Regex("""(\w+ \w+) phosphorylates[^.]*p53""", "x")
    ss.udf.register("RGX2", (text: String) => pattern2.findAllMatchIn(text).map(m => Span(m.start(1)+1, m.end(1)+1)).toList)
  }

  override def experiment(ss: SparkSession): Unit = {
    val results = ss.sql("" +
      "SELECT pubmed_id, x, lower(substring(abstract, x.begin, x.end - x.begin)) AS candidate " +
      "FROM (SELECT pubmed_id, abstract, explode(RGX1(abstract)) AS x FROM abstracts) r " +
      "JOIN aliases a ON lower(substring(abstract, x.begin, x.end - x.begin)) = a.alias " +
      "UNION ALL " +
      "SELECT pubmed_id, x, lower(substring(abstract, x.begin, x.end - x.begin)) AS candidate " +
      "FROM (SELECT pubmed_id, abstract, explode(RGX2(abstract)) AS x FROM abstracts) r " +
      "JOIN aliases a ON lower(substring(abstract, x.begin, x.end - x.begin)) = a.alias")

    val collected: Array[Row] = results.collect()

    log append s"results set size: ${collected.length}\n"
    log append(collected.take(10).mkString("\n") + "\n")
  }
}
