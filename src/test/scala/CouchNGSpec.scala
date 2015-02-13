import akka.actor.ActorSystem
import net.rfc1149.canape._
import org.specs2.mutable._
import org.specs2.specification._

import scala.concurrent.Await
import scala.concurrent.duration._

// This requires a local standard CouchDB instance. The "canape-test-*" databases
// will be created, destroyed and worked into. There must be an "admin"/"admin"
// account.

class CouchNGSpec extends Specification {

  implicit val system = ActorSystem("canape")
  implicit val dispatcher = system.dispatcher

  val couch = new CouchNG(auth = Some("admin", "admin"))

  sequential

  "couch.status()" should {

    "have a version we are comfortable with" in {
      val status = Await.result(couch.status(), (5, SECONDS))
      status.version must startWith("1.")
    }

    "return a list of active tasks" in {
      Await.ready(couch.activeTasks(), (5, SECONDS))
      success
    }

  }

}