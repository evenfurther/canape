package net.rfc1149.canape

import akka.actor.ActorSystem
import akka.http.ConnectionPoolSettings
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.HostConnectionPool
import akka.http.scaladsl.marshalling.{PredefinedToEntityMarshallers, ToEntityMarshaller}
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.MediaTypes.`application/json`
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Accept, Authorization, BasicHttpCredentials, `User-Agent`}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import akka.stream.scaladsl.{Keep, Flow, Sink, Source}
import akka.stream.{ActorFlowMaterializer, FlowMaterializer}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._
import play.api.libs.json._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.Try

/**
 * Connexion to a CouchDB server.
 *
 * @param host the server host name or IP address
 * @param port the server port
 * @param auth an optional (login, password) pair
 */

class Couch(val host: String = "localhost",
            val port: Int = 5984,
            val auth: Option[(String, String)] = None,
            val config: Config = ConfigFactory.load())
           (implicit private[canape] val system: ActorSystem) {

  import Couch._

  private[canape] implicit val dispatcher = system.dispatcher
  private[canape] implicit val fm = ActorFlowMaterializer()

  private[this] val canapeConfig = config.getConfig("canape")
  private[this] val userAgent = `User-Agent`(canapeConfig.as[String]("user-agent"))
  private[this] implicit val timeout: Timeout = canapeConfig.as[FiniteDuration]("request-timeout")

  private[this] lazy val hostConnectionPool: Flow[(HttpRequest, Any), (Try[HttpResponse], Any), HostConnectionPool] =
    Http().newHostConnectionPool[Any](host, port)

  private[this] def blockingHostConnectionPool : Flow[(HttpRequest, Any), (Try[HttpResponse], Any), HostConnectionPool] = {
    val connectionPoolSettings = ConnectionPoolSettings.create(system).copy(maxRetries = 0, pipeliningLimit = 1)
    val pool = Http().newHostConnectionPool[Any](host, port, settings = connectionPoolSettings)
    val forceProtocol =
      Flow[(HttpRequest, Any)].map { case (request, marker) => (request.withProtocol(HttpProtocols.`HTTP/1.0`), marker) }
    forceProtocol.viaMat(pool)(Keep.right)
  }

  /**
   * Send an arbitrary HTTP request on the regular (non-blocking) pool.
   *
   * @param request the request to send
   */
  def sendRequest(request: HttpRequest): Future[HttpResponse] =
    Source.single(request -> null).via(hostConnectionPool).runWith(Sink.head).map(_._1.get)

  /**
   * Send an arbitrary HTTP request on the potentially blocking bool.
   *
   * @param request the request to send
   */
  def sendPotentiallyBlockingRequest(request: HttpRequest): Source[Try[HttpResponse], Unit] =
    // The request is sent in HTTP/1.0 to ensure that no other request will be enqueued on the same outgoing connection
    Source.single(request -> null).via(blockingHostConnectionPool).map(_._1)


  private[this] val defaultHeaders = {
    val authHeader = auth map { case (login, password) => Authorization(BasicHttpCredentials(login, password)) }
    userAgent :: Accept(`application/json`) :: authHeader.toList
  }

  // TODO: it should not be exposed
  private[canape] def Get(query: Uri): HttpRequest = HttpRequest(GET, uri = query, headers = defaultHeaders)

  private[this] def Post[T: ToEntityMarshaller](query: Uri, data: T)(implicit ev: T => RequestEntity): HttpRequest =
    HttpRequest(POST, uri = query, entity = ev(data), headers = defaultHeaders)

  private[this] def Put[T](query: Uri, data: T = HttpEntity.Empty)(implicit ev: T => RequestEntity): HttpRequest =
    HttpRequest(PUT, uri = query, entity = ev(data), headers = defaultHeaders)

  private[this] def Delete(query: Uri): HttpRequest =
    HttpRequest(DELETE, uri = query, headers = defaultHeaders)

  /**
   * Build a GET HTTP request.
   *
   * @param query the query string, including the already-encoded optional parameters
   * @return a future containing the HTTP response
   */
  def makeRawGetRequest(query: Uri): Future[HttpResponse] = sendRequest(Get(query))

  /**
   * Build a GET HTTP request.
   *
   * @param query the query string, including the already-encoded optional parameters
   * @tparam T the type of the result
   * @return a future containing the required result
   */
  def makeGetRequest[T: Reads](query: Uri): Future[T] =
    makeRawGetRequest(query).flatMap(checkResponse[T])

  /**
   * Build a POST HTTP request.
   *
   * @param query the query string, including the already-encoded optional parameters
   * @param data the data to post
   * @tparam T the type of the result
   * @return a future containing the required result
   *
   * @throws CouchError if an error occurs
   */
  def makePostRequest[T: Reads](query: Uri, data: JsObject): Future[T] =
    sendRequest(Post(query, data)).flatMap(checkResponse[T])

  /**
   * Build a POST HTTP request.
   *
   * @param query the query string, including the already-encoded optional parameters
   * @tparam T the type of the result
   * @return A future containing the required result
   *
   * @throws CouchError if an error occurs
   */
  def makePostRequest[T: Reads](query: Uri): Future[T] = {
    // Because of bug COUCHDB-2583, some methods require an empty payload with content-type
    // `application/json`, which is invalid. We will generate it anyway to be compatible
    // with CouchDB 1.6.1.
    val fakeEmptyJsonPayload = HttpEntity(`application/json`, "")
    sendRequest(Post(query, fakeEmptyJsonPayload)).flatMap(checkResponse[T])
  }

  /**
   * Build a POST HTTP request.
   *
   * @param query the query string, including the already-encoded optional parameters
   * @param data the data to post
   * @tparam T the type of the result
   * @return a future containing the required result
   *
   * @throws CouchError if an error occurs
   */
  def makePostRequest[T: Reads](query: Uri, data: FormData): Future[T] = {
    val payload = HttpEntity(ContentType(MediaTypes.`application/x-www-form-urlencoded`), data.fields.toString())
    sendRequest(Post(query, payload)).flatMap(checkResponse[T])
  }

  /**
   * Build a PUT HTTP request.
   *
   * @param query the query string, including the already-encoded optional parameters
   * @param data the data to post
   * @tparam T the type of the result
   * @return a future containing the required result
   *
   * @throws CouchError if an error occurs
   */
  def makePutRequest[T: Reads](query: Uri, data: JsValue): Future[T] =
    sendRequest(Put(query, data)).flatMap(checkResponse[T])

  /**
   * Build a PUT HTTP request.
   *
   * @param query the query string, including the already-encoded optional parameters
   * @tparam T the type of the result
   * @return a future containing the required result
   *
   * @throws CouchError if an error occurs
   */
  def makePutRequest[T: Reads](query: Uri): Future[T] =
    sendRequest(Put(query)).flatMap(checkResponse[T])

  /**
   * Build a DELETE HTTP request.
   *
   * @param query the query string, including the already-encoded optional parameters
   * @tparam T the type of the result
   * @return a future containing the required result
   *
   * @throws CouchError if an error occurs
   */
  def makeDeleteRequest[T: Reads](query: Uri): Future[T] =
    sendRequest(Delete(query)).flatMap(checkResponse[T])

  private[this] def buildURI(fixedAuth: Option[(String, String)]): Uri =
    Uri().withScheme("http").withHost(host).withPort(port).withUserInfo(fixedAuth.map(u => s"${u._1}:${u._2}").getOrElse(""))

  /** URI that refers to the database */
  val uri = buildURI(auth)

  protected def canEqual(that: Any) = that.isInstanceOf[Couch]

  override def equals(that: Any) = that match {
    case other: Couch if other.canEqual(this) => uri == other.uri
    case _ => false
  }

  override def hashCode() = toString.hashCode()

  override def toString = buildURI(auth.map(x => (x._1, "********"))).toString()

  /**
   * Launch a mono-directional replication.
   *
   * @param source the database to replicate from
   * @param target the database to replicate into
   * @param params extra parameters to the request
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def replicate(source: Database, target: Database, params: JsObject = Json.obj()): Future[JsObject] = {
    makePostRequest[JsObject]("/_replicate", params ++ Json.obj("source" -> source.uriFrom(this), "target" -> target.uriFrom(this)))
  }

  /**
   * CouchDB installation status.
   *
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def status(): Future[Status] = makeGetRequest[Status]("/")

  /**
   * CouchDB active tasks.
   *
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def activeTasks(): Future[List[JsObject]] = makeGetRequest[List[JsObject]]("/_active_tasks")

  /**
   * Request UUIDs from the database.
   *
   * @param count the number of UUIDs to return
   * @return a sequence of UUIDs
   */
  def getUUIDs(count: Int): Future[Seq[String]] =
    makeGetRequest[JsObject](s"/_uuids?count=$count") map { r => (r \ "uuids").as[Seq[String]] }

  /**
   * Request an UUID from the database.
   *
   * @return an UUID
   */
  def getUUID: Future[String] = getUUIDs(1).map(_.head)

  /**
   * Get a named database. This does not attempt to connect to the database or check
   * its existence.
   *
   * @param databaseName the database name
   * @return an object representing this database
   */
  def db(databaseName: String) = Database(this, databaseName)

  /**
   * Get the list of existing databases.
   *
   * @return a list of databases on this server
   */
  def databases(): Future[List[String]] = makeGetRequest[List[String]]("/_all_dbs")

  /**
   * Release external resources used by this connector.
   *
   * @return a future which gets completed when the release is done
   */
  def releaseExternalResources(): Future[Unit] =
    Http().shutdownAllConnectionPools()

}

object Couch {

  private[canape] def checkResponse[T: Reads](response: HttpResponse)(implicit fm: FlowMaterializer, ec: ExecutionContext): Future[T] = {
    response.status match {
      case status if status.isFailure() =>
        jsonUnmarshaller[JsObject]().apply(response.entity).map(body => throw new StatusError(status, body))
      case _ =>
        jsonUnmarshaller[T]().apply(response.entity)
    }
  }

  implicit def jsonMarshaller[T: Writes]: ToEntityMarshaller[T] =
    PredefinedToEntityMarshallers.stringMarshaller(`application/json`).compose(implicitly[Writes[T]].writes(_).toString())

  implicit def jsonUnmarshaller[T: Reads]()(implicit fm: FlowMaterializer, ec: ExecutionContext): FromEntityUnmarshaller[T] =
    PredefinedFromEntityUnmarshallers.stringUnmarshaller.forContentTypes(`application/json`)
      .map(s => implicitly[Reads[T]].reads(Json.parse(s)).recoverTotal(e => throw DataError(e)))

  implicit def jsonToEntity[T: Writes](data: T): RequestEntity =
    HttpEntity(`application/json`, implicitly[Writes[T]].writes(data).toString().getBytes("UTF-8"))

  sealed abstract class CouchError extends Exception

  case class DataError(error: JsError) extends CouchError

  case class StatusError(code: Int, error: String, reason: String) extends CouchError {

    def this(status: akka.http.scaladsl.model.StatusCode, body: JsObject) =
      this(status.intValue(), (body \ "error").as[String], (body \ "reason").as[String])

    override def toString = s"StatusError($code, $error, $reason)"

  }

  /**The Couch instance current status. */
  case class Status(couchdb: String,
                    version: String,
                    vendor: Option[VendorInfo])

  case class VendorInfo(name: String,
                        version: String)

  implicit val vendorInfoRead: Reads[VendorInfo] = Json.reads[VendorInfo]
  implicit val statusRead: Reads[Status] = Json.reads[Status]

}
