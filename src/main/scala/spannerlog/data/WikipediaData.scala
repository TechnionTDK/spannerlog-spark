package spannerlog.data

import java.io.File

object WikipediaData {

  def filePath: String = {
    val resource = this.getClass.getClassLoader.getResource("wikipedia/wikipedia.dat")
    if (resource == null) sys.error("Please download the dataset as explained in the assignment instructions")
    new File(resource.toURI).getPath
  }

  var cnt = 1

  def parse(line: String): WikipediaArticle = {
    val subs = "</title><text>"
    val i = line.indexOf(subs)
    val title = line.substring(14, i)
    val text = line.substring(i + subs.length, line.length - 16)
    val id = cnt
    cnt += 1
    WikipediaArticle(id, title, text)
  }
}

case class WikipediaArticle(id: Int, title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}
