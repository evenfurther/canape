package net.rfc1149.canape

import akka.actor.Stash
import akka.actor.Status.Failure
import akka.pattern.pipe
import akka.stream.ActorMaterializer
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import akka.stream.scaladsl.{Keep, Sink, SinkQueue, Source}
import net.ceedubs.ficus.Ficus._
import net.rfc1149.canape.Couch.StatusError
import play.api.libs.json.{JsError, JsObject, JsSuccess, Json}

import scala.concurrent.Promise
import scala.concurrent.duration._

class ChangesSource(database: Database, params: Map[String, String] = Map(), extraParams: JsObject = Json.obj(), initialSeq: Long = -1)
  extends ActorPublisher[JsObject] with Stash {

  import ChangesSource._

  private[this] implicit val executionContext = context.system.dispatcher
  private[this] implicit val materializer = ActorMaterializer()
  private[this] implicit val reconnectionDelay =
    database.couch.config.as[FiniteDuration]("canape.changes-source-reconnection-delay")

  private[this] var ongoingConnection = false
  private[this] var queue: SinkQueue[JsObject] = null
  private[this] var sendInProgress = false

  private[this] var computedInitialSeq: Long = -1
  private[this] var sinceSeq: Long = -1

  private[this] def connect(delay: FiniteDuration) = {
    assert(totalDemand > 0, "unneeded connections to the changes stream")
    assert(queue == null, "queue still exists when reconnecting")
    assert(!sendInProgress, "send in progress while reconnecting")
    assert(sinceSeq >= 0)
    ongoingConnection = true
    val source = Source.empty.initialDelay(delay)
      .concat(database.continuousChanges(params + ("since" -> sinceSeq.toString), extraParams))
    queue = source.toMat(Sink.queue())(Keep.right).run()
    sendFromQueue()
  }

  private[this] def sendFromQueue() = {
    assert(!sendInProgress, "unable to resolve two futures at the same time")
    assert(queue != null)
    sendInProgress = true
    queue.pull().transform(Change, ChangesError).pipeTo(self)
  }

  override def preStart() = {
    sinceSeq = initialSeq
    if (sinceSeq == -1) {
      context.become(receiveInitialSequence)
      self ! GetInitialSequence
    } else {
      computedInitialSeq = initialSeq
    }
  }

  def receiveInitialSequence: Receive = {

    case Cancel =>
      // We have not obtained the initial sequence yet, so let's stop trying
      context.stop(self)

    case GetInitialSequence =>
      database.status().map(js => (js \ "update_seq").as[Long]).transform(InitialSequence, GetInitialSequenceError).pipeTo(self)

    case InitialSequence(n) =>
      assert(sinceSeq == -1)
      sinceSeq = n
      computedInitialSeq = sinceSeq
      context.become(receive)
      unstashAll()

    case Failure(GetInitialSequenceError(t)) =>
      t match {
        case _: StatusError =>
          // An HTTP error means that the database server is alive, but has a problem (for example, the database
          // does not exist). This should be handled downstream, as we do not want to hammer the server.
          onError(t)
          context.stop(self)
        case _ =>
          context.system.scheduler.scheduleOnce(reconnectionDelay, self, GetInitialSequence)
      }

    case _ =>
      stash()

  }

  def receive: Receive = {

    case Cancel =>
      // We have no way of cancelling a SinkQueue request. We have to wait for the connection to timeout, or for
      // the backpressure to act on the TCP layer and make the connection fail.
      context.stop(self)

    case Request(_) =>
      if (!ongoingConnection)
        connect(0.second)
      else if (!sendInProgress)
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
      connect(0.second)

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
          connect(reconnectionDelay)
      }

    case InitialSequencePromise(promise) =>
      promise.success(computedInitialSeq)

  }

}

object ChangesSource {

  private case class Change(change: Option[JsObject])
  private case class ChangesError(throwable: Throwable) extends Exception
  private case object GetInitialSequence
  private case class InitialSequence(initialSequence: Long)
  private case class GetInitialSequenceError(throwable: Throwable) extends Exception
  private[canape] case class InitialSequencePromise(promise: Promise[Long])

}
