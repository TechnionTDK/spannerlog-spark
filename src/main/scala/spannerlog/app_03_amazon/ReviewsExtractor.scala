package spannerlog.app_03_amazon

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import spannerlog.Utils.{isRunningOnLocal, log}
import spannerlog.nlp.{CoreNlp, TextAnnotator}
import spannerlog.{Runner, SparkSessionApp}

import scala.util.matching.Regex


object ReviewsExtractor extends SparkSessionApp {
  var title: String = _
  var split: Boolean = _
  var reviewsCsvPath: String = _
  var isOneSentence:Boolean = _
  @transient var textAnnotator: TextAnnotator = _

  def setExperiment(test: String, split: String, isOneSentence: Boolean): Unit = {
    this.title = s"03-amazon/$test"
    this.split = split.equals("split")
    this.reviewsCsvPath =
      (if (isRunningOnLocal) s"src/main/resources/amazon-fine-food/$split/"
      else Runner.hdfsUrl + s"thesis/benchmark/03-amazon/$test/$split/app-spark/") + "reviews.csv"
    this.isOneSentence = isOneSentence

    log.append(s"title: $title\n")
    log.append(s"split: ${this.split}\n")
    log.append(s"isOneSentence: $isOneSentence\n")
    log.append("Input:\n")
    log.append(s"  $reviewsCsvPath\n\n")

    println("Experiment is Set")

    println("Loading text annotator... ")
    getOrCreateTextAnnotator().sentiment("mock-up")
    println("... done.")
  }

  def getOrCreateTextAnnotator(): TextAnnotator = {
    if (textAnnotator == null)
      this.textAnnotator = new CoreNlp("tokenize, ssplit, pos, parse, sentiment", isOneSentence)
    textAnnotator
  }

  override def preprocess(ss: SparkSession): Unit = {
    assert(reviewsCsvPath != null)

    var abstractsSchemaArray = Array(
      StructField("review_id", IntegerType, nullable = false),
      StructField("product_id", StringType, nullable = false),
      StructField("user_id", StringType, nullable = false),
      StructField("profile_name", StringType, nullable = false),
      StructField("helpfulness_numerator", StringType, nullable = false),
      StructField("helpfulness_denominator", StringType, nullable = false),
      StructField("score", StringType, nullable = false),
      StructField("time", StringType, nullable = false),
      StructField("summary", StringType, nullable = false),
      StructField("text", StringType, nullable = false))
    if (split)
      abstractsSchemaArray = abstractsSchemaArray.patch(9, List(StructField("sentence_no", StringType, nullable = false)), 0)

    val reviews = ss.read
      .option("header", "true")
      .schema(StructType(abstractsSchemaArray))
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(reviewsCsvPath)
      .na.fill("", Seq("text"))
//    reviews.show()
    reviews.cache()
    reviews.createOrReplaceTempView("reviews")

    ss.udf.register("sentiment", (text: String) => getOrCreateTextAnnotator().sentiment(text))

    val pattern1 = new Regex(""".*service.*""", "x")
    ss.udf.register("RGX1", (text: String) => pattern1.findAllMatchIn(text).map(m => true).nonEmpty)
  }

  override def experiment(ss: SparkSession): Unit = {

    val r2 = if (split) {

      val r1 = ss.sql("" +
        "SELECT review_id, sentence_no, score, text " +
        "FROM reviews " +
        "WHERE RGX1(text) = true " +
        ""
      ).cache()

      r1.createOrReplaceTempView("r1")

      val r2 = ss.sql("" +
        "SELECT review_id, sentence_no, score, text, explode(sentiment(text)) as sentiment " +
        "FROM r1 " +
        ""
      )
//        .cache()

      r2.createOrReplaceTempView("r2")

      ss.sql("" +
        "SELECT " +
        "  review_id, " +
        "  sentence_no, " +
        "  score, " +
        "  sentiment.sentimentLabel, " +
        "  substring(text, sentiment.location.begin, sentiment.location.end - sentiment.location.begin) as excerpt " +
        "FROM r2 " +
//        "WHERE RGX1(substring(text, sentiment.location.begin, sentiment.location.end - sentiment.location.begin)) = true " +
        ""
      )

//      val r1 = ss.sql("" +
//        "SELECT review_id, sentence_no, explode(sentiment(text)) as sentiment " +
//        "FROM reviews " +
//        "WHERE RGX1(text) = true " +
//        ""
//      ).cache()
////      r1.show()
//      r1.createOrReplaceTempView("r1")
//
//      ss.sql("" +
//        "SELECT " +
//        "  reviews.review_id, " +
//        "  reviews.sentence_no, " +
//        "  score, " +
//        "  sentiment.sentimentLabel, " +
//        "  text " +
//        "FROM reviews " +
//        "JOIN r1 " +
//        "  ON r1.review_id = reviews.review_id " +
//        "  AND r1.sentence_no = reviews.sentence_no" +
//        ""
//      )
    } else {

      val r1 = ss.sql("" +
        "SELECT review_id, score, text " +
        "FROM reviews " +
        "WHERE RGX1(text) = true " +
        ""
      )
//        .cache()

      r1.createOrReplaceTempView("r1")

      val r2 = ss.sql("" +
      "SELECT review_id, score, text, explode(sentiment(text)) as sentiment " +
      "FROM r1 " +
      ""
      )
//        .cache()

      r2.createOrReplaceTempView("r2")

      ss.sql("" +
        "SELECT " +
        "  review_id, " +
        "  score, " +
        "  sentiment.sentimentLabel, " +
        "  substring(text, sentiment.location.begin, sentiment.location.end - sentiment.location.begin) as excerpt " +
        "FROM r2 " +
        "WHERE RGX1(substring(text, sentiment.location.begin, sentiment.location.end - sentiment.location.begin)) = true " +
        ""
      )

//      val r1 = ss.sql("" +
//        "SELECT review_id, explode(sentiment(text)) as sentiment " +
//        "FROM reviews " +
//        "WHERE RGX1(text) = true " +
//        ""
//      ).cache()
//      //      r1.show()
//      r1.createOrReplaceTempView("r1")
//
//      ss.sql("" +
//        "SELECT " +
//        "  reviews.review_id, " +
//        "  score, " +
//        "  sentiment.sentimentLabel, " +
//        "  substring(text, sentiment.location.begin, sentiment.location.end - sentiment.location.begin) as excerpt " +
//        "FROM reviews " +
//        "JOIN r1 " +
//        "  ON r1.review_id = reviews.review_id " +
//        "WHERE RGX1(substring(text, sentiment.location.begin, sentiment.location.end - sentiment.location.begin)) = true "
//      )
    }

    val collected: Array[Row] = r2.collect()
//    log append (collected.take(10).mkString("\n") + "\n")
    log append s"results set size: ${collected.length}\n"
  }
}
