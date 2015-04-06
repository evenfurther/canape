package net.rfc1149.canape

import akka.actor.{ActorRef, ActorSystem}
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._
import play.api.libs.functional.syntax._
import play.api.libs.json._
import spray.can.Http
import spray.can.Http.{CloseAll, HostConnectorInfo, HostConnectorSetup}
import spray.can.client.{ClientConnectionSettings, HostConnectorSettings}
import spray.http.HttpHeaders.{Accept, Authorization, `User-Agent`}
import spray.http.MediaTypes.`application/json`
import spray.http._
import spray.httpx.PlayJsonSupport
import spray.httpx.RequestBuilding._
import spray.httpx.marshalling.BasicMarshallers

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.implicitConversions

/**
 * Connexion to a CouchDB server.
 *
 * @param host the server host name or IP address
 * @param port the server port
 * @param auth an optional (login, password) pair
 */

class Couch(val host: String = "localhost",
            val port: Int = 5984,
            val auth: Option[(String, String)] = None)
           (implicit system: ActorSystem) extends PlayJsonSupport {

  import net.rfc1149.canape.Couch._

  private[this] implicit val dispatcher = system.dispatcher

  private[this] def connectionSettings(aggregateChunks: Boolean): ClientConnectionSettings =
    ClientConnectionSettings(system).copy(responseChunkAggregationLimit = if (aggregateChunks) 1024 * 1024 else 0)

  private[this] def makeHostConnector(aggregateChunks: Boolean): Future[ActorRef] = {
    // TODO: check gzip handling
    val authHeader = auth map { case (login, password) => Authorization(BasicHttpCredentials(login, password)) }
    val headers = userAgent :: Accept(`application/json`) :: authHeader.toList
    val settings = HostConnectorSettings(system).copy(connectionSettings = connectionSettings(aggregateChunks))
    val setup = HostConnectorSetup(host, port, defaultHeaders = headers, settings = Some(settings))
    IO(Http).ask(setup).mapTo[HostConnectorInfo].map(_.hostConnector)
  }

  private[this] lazy val hostConnector = makeHostConnector(aggregateChunks = true)

  private[this] lazy val chunkedHostConnector = makeHostConnector(aggregateChunks = false)

  private[this] def checkResponse[T: Reads](response: HttpResponse): T = {
    response.status match {
      case status if status.isFailure =>
        throw StatusError(status, Json.parse(response.entity.asString).as[JsObject])
      case _ =>
        Json.parse(response.entity.asString).validate[T] match {
          case JsSuccess(v, _) =>
            v
          case e: JsError =>
            throw DataError(e)
      }
    }
  }

  /**
   * Build a GET HTTP request.
   *
   * @param query The query string, including the already-encoded optional parameters.
   * @tparam T The type of the chunks (if allowChunks is true) or of the result.
   * @return A request.
   */
  def makeGetRequest[T: Reads](query: String, allowChunks: Boolean = false): Future[T] = {
    hostConnector.flatMap(_.ask(Get(query)).mapTo[HttpResponse]).map(checkResponse[T](_))
  }

  /**
   * Build a POST HTTP request.
   *
   * @param query the query string, including the already-encoded optional parameters
   * @param data the data to post
   * @tparam T the type of the result
   * @return a request.
   *
   * @throws CouchError if an error occurs
   */
  def makePostRequest[T: Reads](query: String, data: JsObject): Future[T] =
    hostConnector.flatMap(_.ask(Post(query, data)).mapTo[HttpResponse]).map(checkResponse[T](_))

  /**
   * Build a POST HTTP request.
   *
   * @param query the query string, including the already-encoded optional parameters
   * @tparam T the type of the result
   * @return a request.
   *
   * @throws CouchError if an error occurs
   */
  def makePostRequest[T: Reads](query: String): Future[T] =
    hostConnector.flatMap(_.ask(Post(query, Json.obj())).mapTo[HttpResponse]).map(checkResponse[T](_))

  /**
   * Build a POST HTTP request.
   *
   * @param query the query string, including the already-encoded optional parameters
   * @param data the data to post
   * @tparam T the type of the result
   * @return a request.
   *
   * @throws CouchError if an error occurs
   */
  def makePostRequest[T: Reads](query: String, data: FormData): Future[T] =
    hostConnector.flatMap(_.ask(Post(query, data)(BasicMarshallers.FormDataMarshaller)).mapTo[HttpResponse]).map(checkResponse[T](_))

  /**
   * Build a PUT HTTP request.
   *
   * @param query the query string, including the already-encoded optional parameters
   * @param data the data to post
   * @tparam T the type of the result
   * @return a request.
   *
   * @throws CouchError if an error occurs
   */
  def makePutRequest[T: Reads](query: String, data: JsValue): Future[T] =
    hostConnector.flatMap(_.ask(Put(query, data)).mapTo[HttpResponse]).map(checkResponse[T](_))

  /**
   * Build a PUT HTTP request.
   *
   * @param query the query string, including the already-encoded optional parameters
   * @tparam T the type of the result
   * @return a request.
   *
   * @throws CouchError if an error occurs
   */
  def makePutRequest[T: Reads](query: String): Future[T] =
    hostConnector.flatMap(_.ask(Put(query)).mapTo[HttpResponse]).map(checkResponse[T](_))

  /**
   * Build a PUT HTTP request.
   *
   * @param query the query string, including the already-encoded optional parameters
   * @param data the data to post
   * @tparam T the type of the result
   * @return a request.
   *
   * @throws CouchError if an error occurs
   */
  def makePutRequest[T: Reads](query: String, data: String): Future[T] =
    hostConnector.flatMap(_.ask(Put(query, data)).mapTo[HttpResponse]).map(checkResponse[T](_))

  /**
   * Build a DELETE HTTP request.
   *
   * @param query the query string, including the already-encoded optional parameters
   * @tparam T the type of the result
   * @return a request
   *
   * @throws CouchError if an error occurs
   */
  def makeDeleteRequest[T: Reads](query: String): Future[T] =
    hostConnector.flatMap(_.ask(Delete(query)).mapTo[HttpResponse]).map(checkResponse[T](_))

  /**
   * Send an arbitrary HTTP request and redirect the answer to a given target.
   *
   * @param request the request to send
   * @param target the actor which will receive the response elements
   * @return a future resolved when the connector has been created
   */
  def sendChunkedRequest(request: HttpRequest, target: ActorRef): Future[Unit] =
    chunkedHostConnector.map(_.tell(request, target))

  /** URI that refers to the database */
  val uri = "http://" + auth.map(x => x._1 + ":" + x._2 + "@").getOrElse("") + host + ":" + port

  protected def canEqual(that: Any) = that.isInstanceOf[Couch]

  override def equals(that: Any) = that match {
    case other: Couch if other.canEqual(this) => uri == other.uri
    case _ => false
  }

  override def hashCode() = toString.hashCode()

  override def toString =
    "http://" + auth.map(x => x._1 + ":********@").getOrElse("") + host + ":" + port

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
   */
  def releaseExternalResources() = hostConnector foreach (_ ! CloseAll)
}

object Couch {

  private[canape] val config: Config = ConfigFactory.load().getConfig("canape")
  private[canape] val userAgent = `User-Agent`(config.as[String]("user-agent"))
  private[canape] implicit val timeout: Timeout = config.as[FiniteDuration]("request-timeout")

  sealed abstract class CouchError extends Exception

  case class DataError(error: JsError) extends CouchError

  case class StatusError(code: Int, error: String, reason: String) extends CouchError {
    override def toString = s"StatusError($code, $error, $reason)"
  }

  object StatusError {
    def apply(status: spray.http.StatusCode, body: JsObject): StatusError =
      StatusError(status.intValue, (body \ "error").as[String], (body \ "reason").as[String])
  }

  /**The Couch instance current status. */
  case class Status(couchdb: String,
                    version: String,
                    vendor: Option[VendorInfo])

  case class VendorInfo(name: String,
                        version: String)

  implicit val vendorInfoRead: Reads[VendorInfo] = (
    (__ \ 'name).read[String] and (__ \ 'version).read[String]
    )(VendorInfo.apply _)

  implicit val statusRead: Reads[Status] = (
    (__ \ 'couchdb).read[String] and (__ \ 'version).read[String] and (__ \ 'vendor).readNullable[VendorInfo]
    )(Status.apply _)

}
