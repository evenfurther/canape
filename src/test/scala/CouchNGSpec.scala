import net.rfc1149.canape.CouchNG.StatusError

import scala.concurrent.Await

class ConnectionNGSpec extends DbNGSpecification {

  val dbSuffix = "connectiontest"

  "couch.status()" should {

    "have a version we are comfortable with" in {
      Await.result(couch.status(), timeout).version must startWith("1.")
    }

  }

  "couch.activeTasks()" should {

    "be queryable" in {
      Await.result(couch.activeTasks(), timeout)
      success
    }

  }

  "db.delete()" should {

    "be able to delete an existing database" in {
      Await.result(db.delete(), timeout)
      success
    }

    "fail when we try to delete a non-existing database" in {
      Await.result(db.delete(), timeout)
      Await.result(db.delete(), timeout) must throwA[StatusError]
    }

  }

  "db.create()" should {

    "be able to create a non-existing database" in {
      Await.result(db.delete(), timeout)
      Await.result(db.create(), timeout)
      success
    }

    "fail when trying to create an existing database" in {
      Await.result(db.create(), timeout) must throwA[StatusError]
    }

  }

}
