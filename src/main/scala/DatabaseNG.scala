package net.rfc1149.canape

import net.liftweb.json._
import net.rfc1149.canape.CouchNG.EmptyJson
import spray.http.Uri
import spray.http.Uri.Query

import scala.concurrent.Future

case class DatabaseNG(couch: CouchNG, databaseName: String) {

  import CouchNG.StatusError

  private[canape] val uri = s"${couch.uri}/$databaseName"
  private[this] val localUri = s"/$databaseName"

  override def toString = uri

  override def hashCode = uri.hashCode

  override def canEqual(that: Any) = that.isInstanceOf[DatabaseNG]

  override def equals(that: Any): Boolean = that match {
    case other: DatabaseNG if other.canEqual(this) => uri == other.uri
    case _ => false
  }

  private[canape] def uriFrom(other: CouchNG) = if (couch == other) databaseName else uri

  private[this] def encode(extra: String, properties: Seq[(String, String)] = Seq()) = {
    val base = s"$localUri/$extra"
    if (properties.isEmpty) base else s"$base?${Query(properties: _*).toString()}"
  }

  /**
   * Get the database status.
   *
   * @return a request
   */
  def status(): Future[mapObject] = couch.makeGetRequest[mapObject](localUri, false)

  /**
   * Get the latest revision of an existing document from the database.
   *
   * @param id the id of the document
   * @return a request
   *
   * @throws StatusError if an error occurs
   */
  def apply(id: String): Future[mapObject] =
    couch.makeGetRequest[mapObject](encode(id))

  /**
   * Get a particular revision of an existing document from the database.
   *
   * @param id the id of the document
   * @param rev the revision of the document
   * @return a request
   *
   * @throws StatusError if an error occurs
   */
  def apply(id: String, rev: String): Future[mapObject] =
    couch.makeGetRequest[mapObject](encode(id, Seq("rev" -> rev)))

  /**
   * Get an existing document from the database.
   *
   * @param id the id of the document
   * @param properties the properties to add to the request
   * @return a request
   *
   * @throws StatusError if an error occurs
   */
  def apply(id: String, properties: Map[String, String]): Future[JValue] =
    apply(id, properties.toSeq)

  /**
   * Get an existing document from the database.
   *
   * @param id the id of the document
   * @param properties the properties to add to the request
   * @return a request
   *
   * @throws StatusError if an error occurs
   */
  def apply(id: String, properties: Seq[(String, String)]): Future[JValue] =
    couch.makeGetRequest[JValue](encode(id, properties))

  private[this] def query(id: String, properties: Seq[(String, String)]): Future[Result] =
    couch.makeGetRequest[Result](encode(id, properties))

  /**
   * Query a view from the database.
   *
   * @param design the design document
   * @param name the name of the view
   * @param properties the properties to add to the request
   * @return a request
   *
   * @throws StatusError if an error occurs
   */
  def view(design: String, name: String, properties: Seq[(String, String)] = Seq()): Future[Result] =
    query(s"_design/$design/_view/$name", properties)

  /**
   * Call an update function.
   *
   * @param design the design document
   * @param name the name of the update function
   * @param data the data to pass to the update function
   * @return a request
   *
   * @throws StatusError if an error occurs
   */
  def update(design: String, name: String, id: String, data: Map[String, String]): Future[JValue] = {
    val stringData = Query(data.toSeq: _*).toString()
    couch.makePostRequest[JValue]("%s/_design/%s/_update/%s/%s".format(databaseName, design, name, id), stringData)
  }

  /**
   * Retrieve the list of public documents from the database.
   *
   * @return a request
   *
   * @throws StatusError if an error occurs
   */
  def allDocs(): Future[Result] = allDocs(Map())

  /**
   * Retrieve the list of public documents from the database.
   *
   * @param params the properties to add to the request
   * @return a request
   *
   * @throws StatusError if an error occurs
   */
  def allDocs(params: Map[String, String]): Future[Result] =
    query("_all_docs", params.toSeq)

  /**
   * Create the database.
   *
   * @return a request
   *
   * @throws StatusError if an error occurs
   */
  def create(): Future[JValue] = couch.makePutRequest[JValue](localUri, EmptyJson)

  /**
   * Compact the database.
   *
   * @return a request
   *
   * @throws StatusError if an error occurs
   */
  def compact(): Future[JValue] = couch.makePostRequest[JValue](s"$localUri/_compact", EmptyJson)

  /**
   * Insert documents in bulk mode.
   *
   * @param docs the documents to insert
   * @param allOrNothing force an insertion of all documents
   * @return a request
   *
   * @throws StatusError if an error occurs
   */
  def bulkDocs(docs: Seq[Any], allOrNothing: Boolean = false): Future[JValue] = {
    val args = Map("all_or_nothing" -> allOrNothing, "docs" -> docs)
    couch.makePostRequest[JValue](s"$localUri/_bulk_docs", args)
  }

  private[this] def batchMode(query: String, batch: Boolean) =
    if (batch) query + "?batch=ok" else query

  /**
   * Insert a document into the database.
   *
   * @param doc the document to insert
   * @param id the id of the document if it is known and absent from the document itself
   * @param batch allow the insertion in batch (unchecked) mode
   * @return a request
   *
   * @throws StatusError if an error occurs
   */
  def insert[T <% JObject](doc: T, id: String = null, batch: Boolean = false): Future[JValue] =
    if (id == null)
      couch.makePostRequest[JValue](batchMode(localUri, batch), Some(doc))
    else
      couch.makePutRequest[JValue](batchMode(s"$localUri/$id", batch), Some(doc))

  /**
   * Delete a document from the database.
   *
   * @param id the id of the document
   * @param rev the revision to delete
   * @return a request
   *
   * @throws StatusError if an error occurs
   */
  def delete(id: String, rev: String): Future[JValue] =
    couch.makeDeleteRequest[JValue](s"$localUri/$id?rev=$rev")

  /**
   * Delete a document from the database.
   *
   * @param doc the document which must contains an `_id` and a `_rev` field
   * @return a request
   *
   * @throws StatusError if an error occurs
   */
  def delete[T <% JObject](doc: T): Future[JValue] = {
    val JString(id) = doc \ "_id"
    val JString(rev) = doc \ "_rev"
    delete(id, rev)
  }

  /**
   * Delete a document from the database.
   *
   * @param doc the document which must contains an `_id` and a `_rev` field
   * @return a request
   *
   * @throws StatusError if an error occurs
   */
  def delete(doc: mapObject): Future[JValue] = {
    val JString(id) = doc("_id")
    val JString(rev) = doc("_rev")
    delete(id, rev)
  }

  /**
   * Delete the database.
   *
   * @return a request
   *
   * @throws StatusError if an error occurs
   */
  def delete(): Future[JValue] = couch.makeDeleteRequest[JValue](localUri)

  /**
   * Request the list of changes from the database.
   *
   * @param params the parameters to add to the request
   * @return a request
   *
   * @throws StatusError if an error occurs
   *
   * @note The kind of request (continuous, longpoll, etc.) will determine the
   * result type.
   */
  def changes(params: Map[String, String] = Map()): Future[JValue] =
    // FIXME: this likely cannot work as-is
    couch.makeGetRequest[JValue](encode("_changes", params.toSeq), true)

  /**
   * Ensure that the database has been written to the permanent storage.
   *
   * @return a request
   *
   * @throws StatusError if an error occurs
   */
  def ensureFullCommit(): Future[JValue] =
    couch.makePostRequest[JValue](s"$localUri/_ensure_full_commit", EmptyJson)

  /**
   * Launch a mono-directional replication from another database.
   *
   * @param source the database to replicate from
   * @param params extra parameters to the request
   * @return a request
   *
   * @throws StatusError if an error occurs
   */
  def replicateFrom[T <% JObject](source: DatabaseNG, params: T = Map()): Future[JObject] =
    couch.replicate(source, this, params)

  /**
   * Launch a mono-directional replication to another database.
   *
   * @param target the database to replicate to
   * @param params extra parameters to the request
   * @return a request
   *
   * @throws StatusError if an error occurs
   */
  def replicateTo[T <% JObject](target: DatabaseNG, params: T = Map()): Future[JObject] =
    couch.replicate(this, target, params)

}
