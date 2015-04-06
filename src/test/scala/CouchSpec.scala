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

  "couch.getUUID*()" should {

    "return an UUID with the expected length" in {
      waitForResult(couch.getUUID) must have size 32
    }

    "return distinct UUIDs when called in succession" in {
      val uuid1 = waitForResult(couch.getUUID)
      val uuid2 = waitForResult(couch.getUUID)
      uuid1 must not be equalTo(uuid2)
    }

    "return distinct UUIDs when called in bulk mode" in {
      waitForResult(couch.getUUIDs(50)).distinct must have size 50
    }
  }

}
