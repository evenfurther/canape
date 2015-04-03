package net.rfc1149.canape

import net.liftweb.json._

import scala.concurrent.{ExecutionContext, Future}

package object helpers {

  import implicits._

  type Solver = Seq[mapObject] => mapObject

  def solve(db: Database,
            documents: Seq[JObject])(solver: Solver): Future[JValue] = {
    val mergedDoc = solver(documents.map(_.toMap)) - "_conflicts"
    val JString(id) = mergedDoc("_id")
    val rev = mergedDoc("_rev")
    val deletedDocs = documents map {
      doc =>
        Map("_id" -> id, "_rev" -> doc \ "_rev", "_deleted" -> true)
    } filterNot {
      _("_rev") == rev
    }
    db.bulkDocs(mergedDoc +: deletedDocs, allOrNothing = true)
  }

  def getRevs(db: Database, id: String, revs: Seq[String] = Seq())(implicit context: ExecutionContext): Future[Seq[JObject]] = {
    val revsList = if (revs.isEmpty) "all" else "[" + revs.map("\"" + _ + "\"").mkString(",") + "]"
    db(id, Map("open_revs" -> revsList)) map {
      _.childrenAs[JObject] map (_ \ "ok") map (_.asInstanceOf[JObject])
    }
  }

  def getConflicting(db: Database, doc: JObject)(implicit context: ExecutionContext): Future[Seq[JObject]] = {
    val JString(id) = doc \ "_id"
    val revs = doc.subSeq[String]("_conflicts")
    getRevs(db, id, revs) map { doc +: _ }
  }

  def getConflictingRevs(db: Database, id: String)(implicit context: ExecutionContext): Future[Seq[String]] =
    db(id, Map("conflicts" -> "true")) map { js: JValue =>
      (js \ "_rev").extract[String] +: (js \ "_conflicts").extract[List[String]]
    }

}
