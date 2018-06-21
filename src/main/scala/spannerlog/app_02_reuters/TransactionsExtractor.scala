package spannerlog.app_02_reuters

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import spannerlog.Utils.{isRunningOnLocal, log}
import spannerlog.nlp.{CoreNlp, Span, TextAnnotator}
import spannerlog.{Runner, SparkSessionApp}

import scala.util.matching.Regex


case class PatternMatch(company1: Span, company2: Span)

object TransactionsExtractor extends SparkSessionApp {
  var title: String = _
  var split: Boolean = _
  var articlesCsvPath: String = _
  var isOneSentence:Boolean = _
  @transient var textAnnotator: TextAnnotator = _

  def setExperiment(test: String, split: String, isOneSentence: Boolean): Unit = {
    this.title = s"02-reuters/$test"
    this.split = split.equals("split")
    this.articlesCsvPath =
      (if (isRunningOnLocal) s"src/main/resources/reuters/$split/"
      else Runner.hdfsUrl + s"thesis/benchmark/02-reuters/$test/$split/app-spark/") + "articles.csv"
    this.isOneSentence = isOneSentence

    log.append(s"title: $title\n")
    log.append(s"split: ${this.split}\n")
    log.append(s"isOneSentence: $isOneSentence\n")
    log.append("Input:\n")
    log.append(s"  $articlesCsvPath\n\n")

    println("Experiment is Set")

    println("Loading text annotator... ")
    getOrCreateTextAnnotator().ner("mock-up")
    println("... done.")
  }

  def getOrCreateTextAnnotator(): TextAnnotator = {
    if (textAnnotator == null)
      textAnnotator = new CoreNlp("tokenize, ssplit, pos, lemma, ner", isOneSentence)
    textAnnotator
  }

  override def preprocess(ss: SparkSession): Unit = {
    assert(articlesCsvPath != null)

    var abstractsSchemaArray = Array(
      StructField("id", StringType, nullable = false),
      StructField("title", StringType, nullable = false),
      StructField("authors", StringType, nullable = false),
      StructField("date", StringType, nullable = false),
      StructField("link", StringType, nullable = false),
      StructField("text", StringType, nullable = false))
    if (split)
      abstractsSchemaArray = abstractsSchemaArray.patch(5, List(StructField("sentence_no", StringType, nullable = false)), 0)

    val articles = ss.read
      .option("header", "true")
      .schema(StructType(abstractsSchemaArray))
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(articlesCsvPath)
      .na.fill("", Seq("text"))
    //    articles.show()
    articles.cache()
    articles.createOrReplaceTempView("articles")

    ss.udf.register("sentence", (text: String) => getOrCreateTextAnnotator().sentence(text))
    ss.udf.register("ner", (text: String) => getOrCreateTextAnnotator().ner(text))

    val pattern = new Regex("""([A-Z]\w+) ([Bb]ought|[Pp]urchased|[Aa]cquired) ([A-Z]\w+)""", "x", "y")
    ss.udf.register("RGX", (text: String) => pattern.findAllMatchIn(text).map(m => PatternMatch(Span(m.start(1) + 1, m.end(1) + 1), Span(m.start(3) + 1, m.end(3) + 1))).toList)
  }

  override def experiment(ss: SparkSession): Unit = {
    //    ss.sql("" +
    //      "SELECT id, " +
    //      "  explode(ner(title)) AS entity, " +
    //      "  date, " +
    //      "  title " +
    //      "FROM articles")
    //      .cache()
    //      .createOrReplaceTempView("R1")
    //
    //    val s1 = ss.sql("" +
    //      "SELECT R1.id, " +
    //      "  substring(R1.title, R1.entity.location.begin, R1.entity.location.end - R1.entity.location.begin) AS company1, " +
    //      "  substring(R1.title, T.entity.location.begin, T.entity.location.end - T.entity.location.begin) AS company2, " +
    //      "  substring(R1.title, S.entity.location.begin, S.entity.location.end - S.entity.location.begin) AS amount, " +
    //      "  R1.date " +
    //      "FROM R1 " +
    //      "JOIN R1 T " +
    //      "  ON R1.id = T.id " +
    //      "JOIN R1 S " +
    //      "  ON R1.id = S.id " +
    //      "WHERE R1.entity.category = \"ORGANIZATION\" " +
    //      "  AND T.entity.category = \"ORGANIZATION\" " +
    //      "  AND S.entity.category = \"MONEY\" " +
    //      "  AND R1.entity.location.begin < T.entity.location.begin ")
    //      .cache()
    //    //    s1.show()
    //    s1.createOrReplaceTempView("S1")

    val s2 =
      if (split) {
        val r2 = ss.sql("" +
          "SELECT " +
          "  id, " +
          "  sentence_no AS sentence, " +
          "  explode(ner(text)) AS entity " +
          "FROM articles")
          .cache()
        //      r2.show()
        r2.createOrReplaceTempView("R2")

        ss.sql("" +
          "SELECT R2.id, " +
          "  substring(articles.text, R2.entity.location.begin, R2.entity.location.end - R2.entity.location.begin) AS company1, " +
          "  substring(articles.text, T.entity.location.begin, T.entity.location.end - T.entity.location.begin) AS company2, " +
          "  substring(articles.text, S.entity.location.begin, S.entity.location.end - S.entity.location.begin) AS amount, " +
          "  articles.date " +
          "FROM R2 " +
          "JOIN R2 T " +
          "  ON R2.id = T.id AND R2.sentence = T.sentence " +
          "JOIN R2 S " +
          "  ON R2.id = S.id AND R2.sentence = S.sentence " +
          "JOIN articles " +
          "  ON articles.id = R2.id AND R2.sentence = articles.sentence_no " +
          "WHERE R2.entity.category = \"ORGANIZATION\" " +
          "  AND T.entity.category = \"ORGANIZATION\" " +
          "  AND S.entity.category = \"MONEY\" " +
          "  AND R2.entity.location.begin < T.entity.location.begin"
        )
          .cache()
        //      s2.show()
        //      s2.createOrReplaceTempView("S2")

      } else { // no-split
        val r3 = ss.sql("" +
          "SELECT " +
          "  id, " +
          "  explode(sentence(text)) AS sentence, " +
          "  substring(text, sentence.begin, sentence.end - sentence.begin) AS text " +
          "FROM articles")
          .cache()
        //      r3.show()
        r3.createOrReplaceTempView("R3")

        val r4 = ss.sql("" +
          "SELECT " +
          "  id, " +
          "  sentence, " +
          "  explode(ner(text)) AS entity " +
          "FROM R3 ")
          .cache()
        //      r4.show()
        r4.createOrReplaceTempView("R4")

        ss.sql("" +
          "SELECT R4.id, " +
          "  substring(articles.text, R4.sentence.begin - 1 + R4.entity.location.begin, R4.entity.location.end - R4.entity.location.begin) AS company1, " +
          "  substring(articles.text, T.sentence.begin - 1 + T.entity.location.begin, T.entity.location.end - T.entity.location.begin) AS company2, " +
          "  substring(articles.text, S.sentence.begin -1 + S.entity.location.begin, S.entity.location.end - S.entity.location.begin) AS amount, " +
          "  articles.date " +
          "FROM R4 " +
          "JOIN R4 T " +
          "  ON R4.id = T.id AND R4.sentence = T.sentence " +
          "JOIN R4 S " +
          "  ON R4.id = S.id AND R4.sentence = S.sentence " +
          "JOIN articles " +
          "  ON articles.id = R4.id " +
          "WHERE R4.entity.category = \"ORGANIZATION\" " +
          "  AND T.entity.category = \"ORGANIZATION\" " +
          "  AND S.entity.category = \"MONEY\" " +
          "  AND R4.entity.location.begin < T.entity.location.begin"
        )
          .cache()
        //      s2.show()
        //      s2.createOrReplaceTempView("S2")
      }

    //    val s3 = ss.sql("" +
    //      "SELECT R1.id, " +
    //      "  substring(R1.title, R1.entity.location.begin, R1.entity.location.end - R1.entity.location.begin) AS company1, " +
    //      "  substring(R1.title, T.entity.location.begin, T.entity.location.end - T.entity.location.begin) AS company2, " +
    //      "  substring(R1.title, S.entity.location.begin, S.entity.location.end - S.entity.location.begin) AS amount, " +
    //      "  R1.date " +
    //      "FROM R1 " +
    //      "JOIN R1 T " +
    //      "  ON R1.id = T.id " +
    //      "JOIN R1 S " +
    //      "  ON R1.id = S.id " +
    //      "WHERE R1.entity.category = \"ORGANIZATION\" " +
    //      "  AND T.entity.category = \"ORGANIZATION\" " +
    //      "  AND S.entity.category = \"MONEY\" " +
    //      "  AND R1.entity.location.begin < T.entity.location.begin ")
    //      .cache()
    //    //    s3.show()
    //    s3.createOrReplaceTempView("S3")

    //    val results = ss.sql("" +
    //      "SELECT * " +
    //      "FROM S1 " +
    //      "UNION ALL " +
    //      "SELECT * " +
    //      "FROM S2 " +
    //      "UNION ALL " +
    //      "SELECT * " +
    //      "FROM S3 "
    //    )

    //    results.show()

    val collected: Array[Row] = s2.collect()
    log append (collected.take(10).mkString("\n") + "\n")
    log append s"results set size: ${collected.length}\n"
  }
}
