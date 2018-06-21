package spannerlog.data

import java.io.File

import org.json4s._
import org.json4s.native.JsonMethods

object SignalMediaData {

  var cnt = 1

  def parse(line: String): SignalMediaArticle = {
    implicit val formats = DefaultFormats
    val id = cnt
    cnt += 1
    SignalMediaArticle(id, (JsonMethods.parse(line) \ "content").extract[String])
  }

  def filePath: String = {
    val resource = this.getClass.getClassLoader.getResource("signalmedia/sample-1M.jsonl")
    if (resource == null) sys.error("The Signal Media dataset is missing")
    new File(resource.toURI).getPath
  }
}


case class SignalMediaArticle(id: Int, content: String)