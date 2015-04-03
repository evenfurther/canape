class CouchSpec extends WithDbSpecification("couch") {

  "couch.status()" should {

    "have a version we are comfortable working with" in {
      waitForResult(couch.status()).version must beGreaterThan("1.6.0")
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
