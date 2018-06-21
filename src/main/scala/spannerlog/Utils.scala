package spannerlog

object Utils {

  val log = new StringBuffer
  var total = 0.0

  def timeit[T](label: String, code: => T): T = {
    val start = System.nanoTime()
    val result = code
    val stop = System.nanoTime()
    val time = (stop - start).asInstanceOf[Double] / 1000000000.0
    total += time
    log.append(f"Experiment: $label%-80s\n" +
      f"Elapsed time: $time sec.\n" +
      f"Returned value: $result\n\n")
    result
  }

  def isRunningOnLocal: Boolean = System.getProperty("os.name").contains("Windows")
}
