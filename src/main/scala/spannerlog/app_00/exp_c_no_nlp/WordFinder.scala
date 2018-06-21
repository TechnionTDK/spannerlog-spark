package spannerlog.app_00.exp_c_no_nlp

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spannerlog.Utils.{isRunningOnLocal, timeit}
import spannerlog.data.{SignalMediaArticle, SignalMediaData}
import spannerlog.{Runner, SparkContextApp}


object WordFinder extends SparkContextApp {

  def findNaive(newsRdd: RDD[SignalMediaArticle], w: String): Int = {
    newsRdd.map(a => StringUtils.countMatches(a.content, w)).reduce(_ + _)
  }

  def findWithSplit(newsRdd: RDD[SignalMediaArticle], w: String): Int = {
    newsRdd.flatMap(a => a.content.split('.').filter(s => s.nonEmpty))
      .map(s => StringUtils.countMatches(s, w)).reduce(_ + _)
  }

  /* Experiment: Counting occurrences of a word in new articles  */
  override def experiment(sc: SparkContext): Unit = {
    val newsRdd: RDD[SignalMediaArticle] =
      if (isRunningOnLocal) sc.textFile(SignalMediaData.filePath).map(SignalMediaData.parse).cache()
      else sc.textFile(Runner.hdfsUrl + "datasets/signalmedia/sample-1H.jsonl").map(SignalMediaData.parse).cache()

    timeit("experiment 3a: naive word counter", WordFinder.findNaive(newsRdd, "terror"))
    timeit("experiment 3b: word counter with split()", WordFinder.findWithSplit(newsRdd, "terror"))
  }
}
