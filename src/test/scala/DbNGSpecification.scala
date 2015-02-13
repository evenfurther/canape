import akka.actor.ActorSystem
import java.util.UUID
import net.rfc1149.canape._
import org.specs2.mutable._
import org.specs2.specification._
import scala.concurrent.Await
import scala.concurrent.duration._

// This requires a local standard CouchDB instance. The "canape-test-*" databases
// will be created, destroyed and worked into. There must be an "admin"/"admin"
// account.

trait DbNGSpecification extends Specification with BeforeAfterExample {

  implicit val system = ActorSystem()
  implicit val dispatcher = system.dispatcher
  implicit val timeout: Duration = (5, SECONDS)

  val dbSuffix: String

  lazy val couch = new CouchNG(auth = Some("admin", "admin"))
  lazy val db = couch.db("canape-test-" + dbSuffix + "-" + UUID.randomUUID)

  override def before =
    try {
      Await.ready(db.create(), timeout)
    } catch {
      case _: Exception =>
    }

  override def after =
    try {
      Await.ready(db.delete(), timeout)
    } catch {
      case _: Exception =>
    }

  sequential

}
