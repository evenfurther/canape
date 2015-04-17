package net.rfc1149.canape

import akka.actor.Status.Failure
import akka.actor.{ActorSystem, Props}
import akka.event.Logging
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request, SubscriptionTimeoutExceeded}
import akka.stream.scaladsl.Source
import play.api.libs.json._
import spray.http.Uri.Query
import spray.http._
import spray.httpx.RequestBuilding.Get

import scala.collection.mutable
import scala.concurrent.Future

case class Database(couch: Couch, databaseName: String) {

  import net.rfc1149.canape.Couch._

  val uri = s"${couch.uri}/$databaseName"
  private[this] val localUri = s"/$databaseName"

  override def toString = uri

  override def hashCode = uri.hashCode

  override def canEqual(that: Any) = that.isInstanceOf[Database]

  override def equals(that: Any): Boolean = that match {
    case other: Database if other.canEqual(this) => uri == other.uri
    case _ => false
  }

  def uriFrom(other: Couch) = if (couch == other) databaseName else uri

  private[this] def encode(extra: String, properties: Seq[(String, String)] = Seq()): String = {
    val base = s"$localUri/$extra"
    if (properties.isEmpty) base else s"$base?${Query(properties: _*).toString()}"
  }

  /**
   * Get the database status.
   *
   * @return a request
   */
  def status(): Future[JsObject] = couch.makeGetRequest[JsObject](localUri)

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
   * Query a view from the database.
   *
   * @param design the design document
   * @param name the name of the view
   * @param properties the properties to add to the request
   * @return a request
   *
   * @throws CouchError if an error occurs
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
   * @throws CouchError if an error occurs
   */
  def update(design: String, name: String, id: String, data: Map[String, String]): Future[JsValue] =
    couch.makePostRequest[JsValue](s"$localUri/_design/$design/_update/$name/$id", FormData(data))

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
  def create(): Future[JsValue] = couch.makePutRequest[JsValue](localUri)

  /**
   * Compact the database.
   *
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def compact(): Future[JsValue] = couch.makePostRequest[JsValue](s"$localUri/_compact")

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
    couch.makePostRequest[Seq[JsObject]](s"$localUri/_bulk_docs", args)
  }

  private[this] def batchMode(query: String, batch: Boolean) =
    if (batch) s"$query?batch=ok" else query

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
      couch.makePostRequest[JsValue](batchMode(localUri, batch), doc)
    else
      couch.makePutRequest[JsValue](batchMode(s"$localUri/$id", batch), doc)

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
    couch.makeDeleteRequest[JsValue](s"$localUri/$id?rev=$rev")

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
   * Delete the database.
   *
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def delete(): Future[JsValue] = couch.makeDeleteRequest[JsValue](localUri)

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
    couch.makePostRequest[JsValue](s"$localUri/_ensure_full_commit")

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

  def continuousChanges(params: Map[String, String] = Map())(implicit system: ActorSystem): Source[JsObject, Unit] =
    Source(ActorPublisher(system.actorOf(Props(new ContinuousChangesActor(params)))))

  class ContinuousChangesActor(params: Map[String, String]) extends ActorPublisher[JsObject] {

    private[this] val log = Logging(context.system, this)

    // XXXXX We should probably not handle the queue ourselves and let the buffer with overflow
    // strategies do the job.
    private[this] val queue: mutable.Queue[JsObject] = mutable.Queue()

    private[this] def startRequest(params: Map[String, String]) = {
      val request = Get(encode("_changes", ("feed" -> "continuous") +: params.toSeq))
      couch.sendChunkedRequest(request, self)
    }

    private var lastSeq: Option[Int] = None

    override def preStart() = startRequest(params)

    override def receive = {
      case SubscriptionTimeoutExceeded =>
        context.stop(self)
      case Request(_) if isActive =>
        for (_ <- 1L to totalDemand.min(queue.size))
          onNext(queue.dequeue())
      case Cancel =>
        context.stop(self)
      case start: ChunkedResponseStart =>
        if (start.response.status.isFailure) {
          onError(Database.ChangedError(start.response.status))
          context.stop(self)
        }
      case message: MessageChunk =>
        val stringData: String = new Predef.String(message.data.toByteArray, "UTF-8")
        if (stringData.length > 1) {
          val value = Json.parse(stringData).as[JsObject]
          // If we are about to get a disconnection, reconnect if needed with a "since" specification to
          // ensure that no value will be missed in the interval.
          value \ "last_seq" match {
            case JsNumber(seq) =>
              if (isActive)
                startRequest(params + ("since" -> seq.toString))
            case _ =>
              // Preserve the sequence number in case we restart on a failure.
              value.validate((__ \ 'seq).json.pick[JsNumber]) match {
                case JsSuccess(n, _) =>
                  lastSeq = Some(n.as[Int])
                  if (isActive && totalDemand > 0)
                    onNext(value)
                  else
                    queue += value
                case error: JsError  =>
                  log.warning(s"Received an unknown message: $value")
              }
          }
        }
      case end: ChunkedMessageEnd =>
        if (!isActive)
          context.stop(self)
      case failure: Failure =>
        log.debug(s"Received a failure message, restarting")
        context.system.scheduler.scheduleOnce(couch.changesReconnectionInterval) {
          startRequest(params ++ lastSeq.map(seq => Map("since" -> seq.toString)).getOrElse(Map()))
        } (context.dispatcher)
      case other =>
        log.warning(s"Received unknown message $other")
    }
  }

}

object Database {

  case class ChangedError(status: spray.http.StatusCode) extends Exception

}
