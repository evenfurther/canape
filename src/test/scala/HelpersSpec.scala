import net.liftweb.json._
import net.rfc1149.canape._

class HelpersSpec extends WithDbSpecification("helperstest") {

  import helpers._
  import implicits._

  def makeConflicts(db: Database) =
    waitForResult(db.bulkDocs(Seq(Map("_id" -> "docid", "extra" -> List("one")),
      Map("_id" -> "docid", "extra" -> List("other")),
      Map("_id" -> "docid", "extra" -> List("yet-another"))),
      true))

  "getRevs()" should {

    "return all the revisions" in new freshDb {
      makeConflicts(db)
      waitForResult(getRevs(db, "docid")) must have size(3)
    }

    "return the selected revisions" in new freshDb {
      makeConflicts(db)
      val revs = waitForResult(db("docid", Map("conflicts" -> "true"))).subSeq[String]("_conflicts")
      waitForResult(getRevs(db, "docid", revs)) must have size(2)
    }

  }

  "getConflicting()" should {

    "return the list of conflicting documents by id" in new freshDb {
      makeConflicts(db)
      val doc = waitForResult(db("docid", Map("conflicts" -> "true"))).asInstanceOf[JObject]
      waitForResult(getConflicting(db, doc)) must have size(3)
    }

  }

  "solve()" should {

    "be able to solve a conflict" in new freshDb {
      makeConflicts(db)
      val revs = waitForResult(getConflictingRevs(db, "docid"))
      val docs = waitForResult(getRevs(db, "docid", revs))
      waitForResult(solve(db, docs) {
        docs => docs.head
      })
      waitForResult(getConflictingRevs(db, "docid")) must have size(1)
    }

    "be able to merge documents" in new freshDb {
      makeConflicts(db)
      val revs = waitForResult(getConflictingRevs(db, "docid"))
      val docs = waitForResult(getRevs(db, "docid", revs))
      waitForResult(solve(db, docs) {
        docs =>
          val extra = docs.map {
            _("extra").children.map(_.extract[String])
          }.flatten.sorted
          docs.head + ("extra" -> JArray(extra.map(JString(_)).toList))
      })
      waitForResult(db("docid"))("extra") must be equalTo(parse("""["one", "other", "yet-another"]"""))
    }

  }

}
