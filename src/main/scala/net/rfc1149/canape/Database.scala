package net.rfc1149.canape

import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.{FormData, HttpProtocols, HttpResponse, Uri}
import akka.stream.scaladsl.{FlattenStrategy, Flow, Source}
import akka.util.ByteString
import play.api.libs.json._

import scala.concurrent.Future
import scala.util.{Failure, Success}

case class Database(couch: Couch, databaseName: String) {

  import Couch._
  import Database._

  private[canape] implicit val dispatcher = couch.dispatcher

  private[this] val localPath: Path = Path(s"/$databaseName")
  val uri: Uri = couch.uri.withPath(localPath)

  override def toString = uri.toString()

  override def hashCode = uri.hashCode

  override def canEqual(that: Any) = that.isInstanceOf[Database]

  override def equals(that: Any): Boolean = that match {
    case other: Database if other.canEqual(this) => uri == other.uri
    case _ => false
  }

  def uriFrom(other: Couch): String = if (couch == other) databaseName else uri.toString()

  private def encode(extra: String, properties: Seq[(String, String)] = Seq()): Uri = {
    val components = extra.split('/')
    val base = Uri().withPath(components.foldLeft(localPath)(_ / _))
    if (properties.isEmpty) base else base.withQuery(properties.toMap)
  }

  /**
   * Get the database status.
   *
   * @return a request
   */
  def status(): Future[JsObject] = couch.makeGetRequest[JsObject](encode(""))

  /**
   * Get the latest revision of an existing document from the database.
   *
   * @param id the id of the document
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def apply(id: String): Future[JsObject] =
    couch.makeGetRequest[JsObject](encode(id))

  /**
   * Get a particular revision of an existing document from the database.
   *
   * @param id the id of the document
   * @param rev the revision of the document
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def apply(id: String, rev: String): Future[JsObject] =
    couch.makeGetRequest[JsObject](encode(id, Seq("rev" -> rev)))

  /**
   * Get an existing document from the database.
   *
   * @param id the id of the document
   * @param properties the properties to add to the request
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def apply(id: String, properties: Map[String, String]): Future[JsValue] =
    apply(id, properties.toSeq)

  /**
   * Get an existing document from the database.
   *
   * @param id the id of the document
   * @param properties the properties to add to the request
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def apply(id: String, properties: Seq[(String, String)]): Future[JsValue] =
    couch.makeGetRequest[JsValue](encode(id, properties))

  private[this] def query(id: String, properties: Seq[(String, String)]): Future[Result] =
    couch.makeGetRequest[Result](encode(id, properties))

  /**
   * Query a view from the database but prevent the reduce part from running.
   *
   * @param design the design document
   * @param name the name of the view
   * @param properties the properties to add to the request
   * @return a future containing the result
   *
   * @throws CouchError if an error occurs
   */
  def mapOnly(design: String, name: String, properties: Seq[(String, String)] = Seq()): Future[Result] =
    query(s"_design/$design/_view/$name", properties :+ ("reduce" -> "false"))

  /**
   * Query a view from the database using map/reduce.
   *
   * @param design the design document
   * @param name the name of the view
   * @param properties the properties to add to the request
   * @tparam V the value type
   * @return a future containing a sequence of results
   */
  def view[K: Reads, V: Reads](design: String, name: String, properties: Seq[(String, String)] = Seq()): Future[Seq[(K, V)]] =
    couch.makeGetRequest[JsObject](encode(s"_design/$design/_view/$name", properties)).map(result => (result \ "rows").as[Array[JsValue]] map { row =>
      (row \ "key").as[K] -> (row \ "value").as[V]
    })

  /**
   * Query a list from the database.
   *
   * @param design the design document
   * @param list the name of the list
   * @param view the name of the view whose result will be passed to the list
   * @param properties the properties to add to the request
   * @return a future containing a HTTP response
   */
  def list(design: String, list: String, view: String, properties: Seq[(String, String)] = Seq()): Future[HttpResponse] =
    couch.makeRawGetRequest(encode(s"_design/$design/_list/$list/$view", properties))

  /**
   * Call an update function.
   *
   * @param design the design document
   * @param name the name of the update function
   * @param data the data to pass to the update function
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def update(design: String, name: String, id: String, data: Map[String, String]): Future[JsValue] =
    couch.makePostRequest[JsValue](encode(s"_design/$design/_update/$name/$id"), FormData(data))

  /**
   * Retrieve the list of public documents from the database.
   *
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def allDocs(): Future[Result] = allDocs(Map())

  /**
   * Retrieve the list of public documents from the database.
   *
   * @param params the properties to add to the request
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def allDocs(params: Map[String, String]): Future[Result] =
    query("_all_docs", params.toSeq)

  /**
   * Create the database.
   *
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def create(): Future[JsValue] = couch.makePutRequest[JsValue](encode(""))

  /**
   * Compact the database.
   *
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def compact(): Future[JsValue] = couch.makePostRequest[JsValue](encode("_compact"))

  /**
   * Insert documents in bulk mode.
   *
   * @param docs the documents to insert
   * @param allOrNothing force an insertion of all documents
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def bulkDocs(docs: Seq[JsObject], allOrNothing: Boolean = false): Future[Seq[JsObject]] = {
    val args = Json.obj("all_or_nothing" -> JsBoolean(allOrNothing), "docs" -> docs)
    couch.makePostRequest[Seq[JsObject]](encode("_bulk_docs"), args)
  }

  private[this] def batchMode(query: Uri, batch: Boolean) =
    if (batch) query.withQuery(("batch" -> "ok") +: query.query) else query

  /**
   * Insert a document into the database.
   *
   * @param doc the document to insert
   * @param id the id of the document if it is known and absent from the document itself
   * @param batch allow the insertion in batch (unchecked) mode
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def insert(doc: JsObject, id: String = null, batch: Boolean = false): Future[JsValue] =
    if (id == null)
      couch.makePostRequest[JsValue](batchMode(encode(""), batch), doc)
    else
      couch.makePutRequest[JsValue](batchMode(encode(id), batch), doc)

  /**
   * Delete a document from the database.
   *
   * @param id the id of the document
   * @param rev the revision to delete
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def delete(id: String, rev: String): Future[JsValue] =
    couch.makeDeleteRequest[JsValue](encode(id, Seq("rev" -> rev)))

  /**
   * Delete multiple revisions of a document from the database. There will be no error if the document does not exist.
   *
   * @param id the id of the document
   * @param revs the revisions to delete (may be empty)
   * @param allOrNothing `true` if all deletions must succeed or fail atomically
   * @return a list of revisions that have been succesfully deleted
   */
  def delete(id: String, revs: Seq[String], allOrNothing: Boolean = false): Future[Seq[String]] =
    revs match {
      case Nil =>
        Future.successful(Nil)
      case revs@(rev :: Nil) =>
        delete(id, rev).map(_ => revs).recover { case _ => Seq() }
      case _ =>
        bulkDocs(revs.map(rev => Json.obj("_id" -> id, "_rev" -> rev, "_deleted" -> true)), allOrNothing = allOrNothing)
          .map(_.collect { case doc if !doc.keys.contains("error") => (doc \ "rev").as[String] })
    }

  /**
   * Delete a document from the database.
   *
   * @param doc the document which must contains an `_id` and a `_rev` field
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def delete[T](doc: JsObject): Future[JsValue] = {
    val id = (doc \ "_id").as[String]
    val rev = (doc \ "_rev").as[String]
    delete(id, rev)
  }

  /**
   * Delete all revisions of a document from the database. There will be no error if the document does not exist.
   *
   * @param id the id of the document
   * @return a future with all the document revisions that have been deleted
   */
  def delete(id: String): Future[Seq[String]] = {
    this(id, Seq("conflicts" -> "true")).map(doc => (doc \ "_rev").as[String] :: (doc \ "_conflicts").asOpt[List[String]].getOrElse(Nil)) flatMap {
      delete(id, _, allOrNothing = false)
    }
  }

  /**
   * Delete the database.
   *
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def delete(): Future[JsValue] = couch.makeDeleteRequest[JsValue](encode(""))

  /**
   * Request the list of changes from the database in a non-continuous way.
   *
   * @param params the parameters to add to the request
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def changes(params: Map[String, String] = Map()): Future[JsValue] =
    couch.makeGetRequest[JsValue](encode("_changes", params.toSeq))

  def revs_limit(limit: Int): Future[JsValue] =
    couch.makePutRequest[JsValue](encode("_revs_limit"), JsNumber(limit))

  def revs_limit(): Future[Long] =
    couch.makeGetRequest[Long](encode("_revs_limit"))

  /**
   * Ensure that the database has been written to the permanent storage.
   *
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def ensureFullCommit(): Future[JsValue] =
    couch.makePostRequest[JsValue](encode("_ensure_full_commit"))

  /**
   * Launch a mono-directional replication from another database.
   *
   * @param source the database to replicate from
   * @param params extra parameters to the request
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def replicateFrom(source: Database, params: JsObject = Json.obj()): Future[JsObject] =
    couch.replicate(source, this, params)

  /**
   * Launch a mono-directional replication to another database.
   *
   * @param target the database to replicate to
   * @param params extra parameters to the request
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def replicateTo(target: Database, params: JsObject = Json.obj()): Future[JsObject] =
    couch.replicate(this, target, params)

  /**
   * Return a continuous changes stream.
   *
   * @param params extra parameters to the request
   * @return a source containing the changes
   */
  def continuousChanges(params: Map[String, String] = Map()): Source[JsObject, Unit] = {
    val request = couch.Get(encode("_changes", (params + ("feed" -> "continuous")).toSeq)).withProtocol(HttpProtocols.`HTTP/1.0`)
    couch.sendChunkedRequest(request).map {
      case Success(response) if response.status.isSuccess() =>
        response.entity.dataBytes
      case Success(response) =>
        sys.error(response.status.reason())
      case Failure(t) =>
        throw t
    }.flatten(FlattenStrategy.concat).via(filterJson)
  }

}

object Database {

  case class ChangedError(status: akka.http.scaladsl.model.StatusCode) extends Exception

  /**
   * Filter objects that contains a `seq` field. To be used with [[Database#continousChanges]].
   */
  val onlySeq: Flow[JsObject, JsObject, Unit] = Flow[JsObject].filter(_.keys.contains("seq"))

  private val filterJson: Flow[ByteString, JsObject, Unit] =
    Flow[ByteString].mapConcat { bs =>
      new String(bs.toArray, "UTF-8").split("\r?\n").filter(_.length > 1).map(Json.parse(_).as[JsObject]).toList
    }

}
