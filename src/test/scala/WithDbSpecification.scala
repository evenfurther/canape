import java.util.UUID

import akka.actor.ActorSystem
import akka.event.Logging
import net.rfc1149.canape.Couch.StatusError
import net.rfc1149.canape._
import org.specs2.mutable._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

// This requires a local standard CouchDB instance. The "canape-test-*" databases
// will be created, destroyed and worked into. There must be an "admin"/"admin"
// account.

abstract class WithDbSpecification(dbSuffix: String) extends Specification {

  implicit val system = ActorSystem("canape-test")
  implicit val dispatcher = system.dispatcher
  implicit val timeout: Duration = (5, SECONDS)

  val couch = new Couch(auth = Some("admin", "admin"))

  trait freshDb extends BeforeAfter {

    val db = couch.db(s"canape-test-$dbSuffix-${UUID.randomUUID()}")
    val log = Logging(system, dbSuffix)
    var _waitEventually: List[Future[Any]] = Nil

    override def before = Await.ready(db.create(), timeout)

    override def after =
      try {
        Await.ready(Future.sequence(_waitEventually), timeout)
        Await.ready(db.delete(), timeout)
      } catch {
        case _: StatusError =>
      }

    def waitEventually[T](fs: Future[T]*): Unit = _waitEventually ++= fs
  }

  def waitForResult[T](f: Future[T]): T = Await.result(f, timeout)
  def waitForEnd[T](fs: Future[T]*): Unit = Await.ready(Future.sequence(fs), timeout)

}
