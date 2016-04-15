import net.rfc1149.canape._
import play.api.libs.json._

class HelpersSpec extends WithDbSpecification("helpers") {

  import helpers._
  import implicits._

  def makeConflicts(db: Database) =
    waitForResult(db.bulkDocs(
      Seq(
        Json.obj("_id" → "docid", "extra" → List("one")),
        Json.obj("_id" → "docid", "extra" → List("other")),
        Json.obj("_id" → "docid", "extra" → List("yet-another"))
      ),
      true
    ))

  "getRevs()" should {

    "return all the revisions" in new freshDb {
      makeConflicts(db)
      waitForResult(getRevs(db, "docid")) must have size (3)
    }

    "return the selected revisions" in new freshDb {
      makeConflicts(db)
      val revs = (waitForResult(db("docid", Map("conflicts" → "true"))) \ "_conflicts").as[Seq[String]]
      waitForResult(getRevs(db, "docid", revs)).map(d ⇒ (d \ "_rev").as[String]).distinct must have size (2)
    }

  }

  "getConflicting()" should {

    "return the list of conflicting documents by id" in new freshDb {
      makeConflicts(db)
      val doc = waitForResult(db("docid", Map("conflicts" → "true"))).asInstanceOf[JsObject]
      val versions = waitForResult(getConflicting(db, doc))
      versions.map(d ⇒ (d \ "_id").as[String]).distinct must have size (1)
      versions.map(d ⇒ (d \ "_rev").as[String]).distinct must have size (3)
    }

  }

  "withIdRev()" should {

    "work with the strings version" in {
      val result = Json.obj("foo" → "bar").withIdRev("docid", "docrev")
      result must be equalTo Json.obj("foo" → "bar", "_id" → "docid", "_rev" → "docrev")
    }

    "work with the strings version and remove older id and rev" in {
      val result = Json.obj("foo" → "bar", "_id" → "foo", "_rev" → "3-bar").withIdRev("docid", "docrev")
      result must be equalTo Json.obj("foo" → "bar", "_id" → "docid", "_rev" → "docrev")
    }

    "work with the reference document version" in {
      val refdoc = Json.obj("_id" → "docid", "_rev" → "docrev")
      val result = Json.obj("foo" → "bar").withIdRev(refdoc)
      result must be equalTo Json.obj("foo" → "bar", "_id" → "docid", "_rev" → "docrev")
    }

    "work with the reference document version and remove older id and rev" in {
      val refdoc = Json.obj("_id" → "docid", "_rev" → "docrev")
      val result = Json.obj("foo" → "bar", "_id" → "foo", "_rev" → "3-bar").withIdRev(refdoc)
      result must be equalTo Json.obj("foo" → "bar", "_id" → "docid", "_rev" → "docrev")
    }
  }

  "solve()" should {

    "be able to solve a conflict by selecting one document" in new freshDb {
      makeConflicts(db)
      val revs = waitForResult(getConflictingRevs(db, "docid"))
      val docs = waitForResult(getRevs(db, "docid", revs))
      waitForResult(solve(db, docs) {
        docs ⇒ docs.head
      })
      waitForResult(getConflictingRevs(db, "docid")) must have size (1)
    }

    "be able to solve a conflict by merging documents" in new freshDb {
      makeConflicts(db)
      val revs = waitForResult(getConflictingRevs(db, "docid"))
      val docs = waitForResult(getRevs(db, "docid", revs))
      waitForResult(solve(db, docs) {
        docs ⇒
          val extra = docs.flatMap { d ⇒
            (d \ "extra").as[Array[String]]
          }.sorted
          docs.head - "extra" ++ Json.obj("extra" → extra)
      })
      waitForResult(getConflictingRevs(db, "docid")) must have size (1)
      (waitForResult(db("docid")) \ "extra").get must be equalTo (Json.parse("""["one", "other", "yet-another"]"""))
    }

  }

}
