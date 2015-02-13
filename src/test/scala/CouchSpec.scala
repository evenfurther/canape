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

  "db.delete()" should {

    "be able to delete an existing database" in new freshDb {
      waitForResult(db.delete())
      success
    }

    "fail when we try to delete a non-existing database" in new freshDb {
      waitForResult(db.delete())
      waitForResult(db.delete()) must throwA[StatusError]
    }

  }

  "db.create()" should {

    "be able to create a non-existing database" in new freshDb {
      waitForResult(db.delete())
      waitForResult(db.create())
      success
    }

    "fail when trying to create an existing database" in new freshDb {
      waitForResult(db.create()) must throwA[StatusError]
    }

  }

}
