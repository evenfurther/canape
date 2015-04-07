package net.rfc1149.canape

import play.api.libs.functional.syntax._
import play.api.libs.json._
import play.api.libs.json.Reads._

import scala.concurrent.{ExecutionContext, Future}

package object helpers {

  private val deleter =
    ((__ \ '_id).json.pickBranch and (__ \ '_rev).json.pickBranch and (__ \ '_deleted).json.put(JsBoolean(true))).reduce

  private val unconflicter = (__ \ '_conflicts).json.prune

  def solve(db: Database, documents: Seq[JsObject])(solver: Seq[JsObject] => JsObject): Future[Seq[JsObject]] = {
    val mergedDoc = solver(documents)
    val rev = mergedDoc \ "_rev"
    val bulkDocs = documents map {
      case d if (d \ "_rev") == rev => mergedDoc.transform(unconflicter).get
      case d                        => d.transform(deleter).get
    }
    db.bulkDocs(bulkDocs, allOrNothing = true)
  }

  def getRevs(db: Database, id: String, revs: Seq[String] = Seq())(implicit context: ExecutionContext): Future[Seq[JsObject]] = {
    val revsList = if (revs.isEmpty) "all" else s"[${revs.map(r => s""""$r"""").mkString(",")}]"
    db(id, Map("open_revs" -> revsList)) map (_.as[Seq[JsObject]].collect {
      case j if j.keys.contains("ok") => (j \ "ok").as[JsObject]
    })
  }

  def getConflicting(db: Database, doc: JsObject)(implicit context: ExecutionContext): Future[Seq[JsObject]] = {
    val JsString(id) = doc \ "_id"
    val revs = (doc \ "_conflicts").as[Seq[String]]
    getRevs(db, id, revs) map { doc +: _ }
  }

  def getConflictingRevs(db: Database, id: String)(implicit context: ExecutionContext): Future[Seq[String]] =
    db(id, Map("conflicts" -> "true")) map { js: JsValue =>
      (js \ "_rev").as[String] +: (js \ "_conflicts").as[Option[List[String]]].getOrElse(Seq())
    }

}
