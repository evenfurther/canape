package net.rfc1149.canape

import dispatch._
import dispatch.json._
import dispatch.json.Js._
import JsHttp._
import sjson.json.JsBean

case class Couch(val host: String = "localhost", val port: Int = 5984, val auth: Option[(String, String)] = None) {

  val uri = "http://" + auth.map(x => x._1 + ":" + x._2 + "@").getOrElse("") + host + ":" + port

  val couchRequest = {
    val base = :/(host, port)
    auth match {
      case Some((login, password)) => base.as_!(login, password)
      case None                    => base
    }
  }

  def replicate(source: Db, target: Db, continuous: Boolean) = {
    val params = Map("source" -> source.uriFrom(this), "target" -> target.uriFrom(this),
		     "continuous" -> continuous)
    couchRequest / "_replicate" << (JsBean.toJSON(params), "application/json") >|
  }

}

object Couch {

  def apply(host: String, port: Int, login: String, password: String): Couch = Couch(host, port, Some((login, password)))

  def apply(login: String, password: String): Couch = Couch(auth = Some((login, password)))

  def apply(host: String, login: String, password: String): Couch = Couch(host, auth = Some((login, password)))

}

class DbStatus(js: JsValue) extends Js {

  val db_name = ('db_name ! str)(js)
  val doc_count = ('doc_count ! num)(js)
  val doc_del_count = ('doc_del_count ! num)(js)
  val update_seq = ('update_seq ! num)(js)
  val purge_seq = ('purge_seq ! num)(js)
  val compact_running = ('compact_running ! bool)(js)
  val disk_size = ('disk_size ! num)(js)
  val data_size = ('data_size ! num)(js)
  val instance_start_time = ('instance_start_time ! str)(js) toLong
  val disk_format_version = ('disk_format_version ! num)(js)
  val committed_update_seq = ('committed_update_seq ! num)(js)

}

case class Db(val couch: Couch, val database: String) extends Request(couch.couchRequest) with Js {

  val uri = couch.uri + "/" + database

  def uriFrom(other: Couch) = if (couch == other) database else uri

  def status = this ># (new DbStatus(_))

}
