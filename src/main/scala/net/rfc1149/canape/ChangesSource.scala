package net.rfc1149.canape

import akka.Done
import akka.actor.Status.Failure
import akka.pattern.pipe
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import akka.stream.scaladsl.{Keep, Sink, SinkQueue}
import net.ceedubs.ficus.Ficus._
import net.rfc1149.canape.Couch.StatusError
import play.api.libs.json.{JsError, JsObject, JsSuccess, Json}

import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration

class ChangesSource(database: Database, params: Map[String, String] = Map(), extraParams: JsObject = Json.obj(), private var sinceSeq: Long = -1)
  extends ActorPublisher[JsObject] {

  import ChangesSource._

  private[this] implicit val executionContext = context.system.dispatcher
  private[this] implicit val materializer = ActorMaterializer()
  private[this] implicit val reconnectionDelay =
    database.couch.config.as[FiniteDuration]("canape.changes-source-reconnection-delay")

  private[this] var ongoingConnection = false
  private[this] var queue: SinkQueue[JsObject] = null
  private[this] var sendInProgress = false

  private[this] val connectionEstablished = Promise[Done]

  private[this] def connect() = {
    assert(totalDemand > 0, "unneeded connections to the changes stream")
    assert(queue == null, "queue still exists when reconnecting")
    assert(!sendInProgress, "send in progress while reconnecting")
    ongoingConnection = true
    val requestSinceSeq = if (sinceSeq == -1) "now" else sinceSeq.toString
    queue = database.continuousChanges(params + ("since" -> requestSinceSeq), extraParams)
        .mapMaterializedValue(done => done.onSuccess { case d => connectionEstablished.trySuccess(d) })
        .toMat(Sink.queue())(Keep.right).run()
    sendFromQueue()
  }

  private[this] def sendFromQueue() = {
    assert(!sendInProgress, "unable to resolve two futures at the same time")
    sendInProgress = true
    queue.pull().transform(Change, ChangesError).pipeTo(self)
  }

  def receive: Receive = {

    case Cancel =>
      // We have no way of cancelling a SinkQueue request. We have to wait for the connection to timeout, or for
      // the backpressure to act on the TCP layer and make the connection fail.
      context.stop(self)

    case Request(n) =>
      if (!ongoingConnection)
        connect()
      else if (n == totalDemand)
        // We had a total demand of 0, which means that nobody is waiting for the next value
        sendFromQueue()

    case Change(Some(change)) =>
      sendInProgress = false
      assert(totalDemand > 0)
      (change \ "seq").validate[Long] match {
        case JsSuccess(n, _) =>
          sinceSeq = n
          onNext(change)
          if (totalDemand > 0)
            sendFromQueue()
        case _: JsError =>
          sendFromQueue()
      }

    case Change(None) =>
      sendInProgress = false
      queue = null
      assert(totalDemand > 0)
      connect()

    case Failure(ChangesError(t)) =>
      sendInProgress = false
      queue = null
      assert(totalDemand > 0)
      t match {
        case _: StatusError =>
          // We do not want to continue in the presence of a HTTP error. See above.
          onError(t)
          context.stop(self)
        case _ =>
          context.system.scheduler.scheduleOnce(reconnectionDelay, self, Reconnect)
      }

    case Reconnect =>
      connect()

    case ConnectionPromise(promise) =>
      promise.completeWith(connectionEstablished.future)

  }

}

object ChangesSource {

  private case class Change(change: Option[JsObject])
  private case class ChangesError(throwable: Throwable) extends Exception
  private case object Reconnect
  private[canape] case class ConnectionPromise(promise: Promise[Done])

}
