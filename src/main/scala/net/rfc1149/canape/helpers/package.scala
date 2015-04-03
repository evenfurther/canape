package net.rfc1149.canape

import play.api.libs.json._

import scala.concurrent.{ExecutionContext, Future}

package object helpers {

  type Solver = Seq[JsObject] => JsObject

  def solve(db: Database, documents: Seq[JsObject])(solver: Solver): Future[JsValue] = {
    val mergedDoc = solver(documents) - "_conflicts"
    val id = mergedDoc \ "_id"
    val rev = mergedDoc \ "_rev"
    val bulkDocs = documents map {
      case d if (d \ "_rev") != rev =>
        Json.obj("_id" -> id, "_rev" -> d \ "_rev", "_deleted" -> JsBoolean(true))
      case _ =>
        mergedDoc
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
