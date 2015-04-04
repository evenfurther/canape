import net.rfc1149.canape.Couch.StatusError

import scala.concurrent.Future

class CouchSpec extends WithDbSpecification("couch") {

  "couch.status()" should {

    "have a version we are comfortable with" in {
      waitForResult(couch.status()).version must startWith("1.")
    }

  }

  "couch.activeTasks()" should {

    "be queryable" in {
      waitForResult(couch.activeTasks())
      success
    }

  }

  "couch.databases()" should {

    "contain the current database in the list" in new freshDb {
      waitForResult(couch.databases()) must contain(db.databaseName)
    }

  }

}
