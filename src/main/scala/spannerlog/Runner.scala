package spannerlog

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import spannerlog.Utils.{isRunningOnLocal, log, timeit, total}
import spannerlog.app_01_pubmed.KinasesExtractor
import spannerlog.app_02_reuters.TransactionsExtractor
import spannerlog.app_03_amazon.ReviewsExtractor
import spannerlog.app_04_splitter_framework.TriGramExtractor

object Runner {

  val hdfsUrl: String = "hdfs://tdkambarimaster:8020/user/yoavn/"

  def main(args: Array[String]): Unit = {
//    org.apache.log4j.BasicConfigurator.configure()

    val ss: SparkSession = SparkSession
      .builder()
      .appName("Spannerlog")
//      .config("spark.master", "local")
      .getOrCreate()

    ss.conf.set("spark.sql.broadcastTimeout", 70000)



    if (args.length == 0)
      throw new IllegalArgumentException("At least one argument is expected but none were passed")

    args(0) match {
      case "01-pubmed" =>
        if (args.length != 3)
          throw new IllegalArgumentException(s"Three arguments are expected but ${args.length} were passed")
        KinasesExtractor.ss = ss
        KinasesExtractor.setExperiment(args(1), args(2))
        KinasesExtractor.compile()
        KinasesExtractor.run()
      case "02-reuters" =>
        if (args.length != 4)
          throw new IllegalArgumentException(s"Four arguments are expected but ${args.length} were passed")
        TransactionsExtractor.ss = ss
        TransactionsExtractor.setExperiment(args(1), args(2), args(3).toBoolean)
        TransactionsExtractor.compile()
        TransactionsExtractor.run()
      case "03-amazon" =>
        if (args.length != 4)
          throw new IllegalArgumentException(s"Four arguments are expected but ${args.length} were passed")
        ReviewsExtractor.ss = ss
        ReviewsExtractor.setExperiment(args(1), args(2), args(3).toBoolean)
        ReviewsExtractor.compile()
        ReviewsExtractor.run()
      case "04-splitter-framework" =>
        if (args.length != 4)
          throw new IllegalArgumentException(s"Four arguments are expected but ${args.length} were passed")
        TriGramExtractor.ss = ss
        TriGramExtractor.setExperiment(args(1), args(2), args(3).toBoolean)
        TriGramExtractor.compile()
        TriGramExtractor.run()
    }
    log.append(s"\nTotal time: $total\n")
    println(log)
  }
}

abstract class SparkSessionApp {

  var ss: SparkSession = _
  def preprocess(ss: SparkSession): Unit
  def experiment(ss: SparkSession): Unit

  def compile(): Unit = timeit("compile", preprocess(ss))

  def run(): Unit = {
    timeit("run", experiment(ss))
    ss.stop()
  }
}


abstract class SparkContextApp {
  def experiment(sc: SparkContext): Unit

  def run(): Unit = {
    val conf: SparkConf =
    if (isRunningOnLocal) new SparkConf().setMaster("local[*]").setAppName("my app")
    else new SparkConf().setAppName("my app")

    val sc: SparkContext = new SparkContext(conf)

    experiment(sc)

    println(log)
    sc.stop()
  }
}

