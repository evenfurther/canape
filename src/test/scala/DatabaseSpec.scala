import akka.actor.ActorSystem
import akka.stream.ActorFlowMaterializer
import akka.stream.scaladsl.Source
import net.liftweb.json._
import net.rfc1149.canape.Couch.StatusError
import net.rfc1149.canape._

import scala.concurrent.Future

class DatabaseSpec extends WithDbSpecification("db") {

  import implicits._

  private def insertedId(f: Future[JValue]): Future[String] = f map { js => (js \ "id").extract[String] }

  private def insertedRev(f: Future[JValue]): Future[String] = f map { js => (js \ "rev").extract[String] }

  private def inserted(f: Future[JValue]): Future[(String, String)] =
    for (id <- insertedId(f); rev <- insertedRev(f)) yield (id, rev)

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

  "db.insert()" should {

    "be able to insert a new document with an explicit id" in new freshDb {
      waitForResult(insertedId(db.insert(JObject(Nil), "docid"))) must be equalTo "docid"
    }

    "be able to insert a new document with an explicit id in batch mode" in new freshDb {
      waitForResult(insertedId(db.insert(JObject(Nil), "docid", true))) must be equalTo "docid"
    }

    "be able to insert a new document with an implicit id" in new freshDb {
      waitForResult(insertedId(db.insert(JObject(Nil)))) must be matching "[0-9a-f]{32}"
    }

    "be able to insert a new document with an implicit id in batch mode" in new freshDb {
      waitForResult(insertedId(db.insert(JObject(Nil), batch = true))) must be matching "[0-9a-f]{32}"
    }

    "be able to insert a new document with an embedded id" in new freshDb {
      waitForResult(insertedId(db.insert(Map("_id" -> "docid")))) must be equalTo "docid"
    }

    "be able to insert a new document with an embedded id in batch mode" in new freshDb {
      waitForResult(insertedId(db.insert(Map("_id" -> "docid"), batch = true))) must be equalTo "docid"
    }

    "be able to update a document with an embedded id" in new freshDb {
      waitForResult(insertedId(db.insert(Map("_id" -> "docid")))) must be equalTo "docid"
      val updatedDoc = waitForResult(db("docid")) + ("foo" -> "bar")
      waitForResult(insertedId(db.insert(updatedDoc))) must be equalTo "docid"
    }

    "be able to update a document with an embedded id in batch mode" in new freshDb {
      waitForResult(insertedId(db.insert(Map("_id" -> "docid")))) must be equalTo "docid"
      val updatedDoc = waitForResult(db("docid")) + ("foo" -> "bar")
      waitForResult(insertedId(db.insert(updatedDoc, batch = true))) must be equalTo "docid"
    }

    "return a consistent rev" in new freshDb {
      waitForResult(insertedRev(db.insert(JObject(Nil), "docid"))) must be matching "1-[0-9a-f]{32}"
    }

  }

  "db.apply()" should {

    "be able to retrieve the content of a document" in new freshDb {
      val id = waitForResult(insertedId(db.insert(Map("key" -> "value"))))
      val doc = waitForResult(db(id))
      doc("key").extract[String] must be equalTo "value"
    }

    "be able to retrieve an older revision of a document with two params" in new freshDb {
      val (id, rev) = waitForResult(inserted(db.insert(Map("key" -> "value"))))
      val doc = waitForResult(db(id))
      waitForResult(db.insert(doc + ("key" -> "newValue")))
      waitForResult(db(id, rev))("key").extract[String] must be equalTo "value"
    }

    "be able to retrieve an older revision of a document with a params map" in new freshDb {
      val (id, rev) = waitForResult(inserted(db.insert(Map("key" -> "value"))))
      val doc = waitForResult(db(id))
      waitForResult(db.insert(doc + ("key" -> "newValue")))
      (waitForResult(db(id, Map("rev" -> rev))) \ "key").extract[String] must be equalTo "value"
    }

    "be able to retrieve an older revision of a document with a params sequence" in new freshDb {
      val (id, rev) = waitForResult(inserted(db.insert(Map("key" -> "value"))))
      val doc = waitForResult(db(id))
      waitForResult(db.insert(doc + ("key" -> "newValue")))
      (waitForResult(db(id, Seq("rev" -> rev))) \ "key").extract[String] must be equalTo "value"
    }

  }

  "db.delete()" should {

    "be able to delete a document" in new freshDb {
      val (id, rev) = waitForResult(inserted(db.insert(JObject(Nil))))
      waitForResult(db.delete(id, rev))
      waitForResult(db(id)) must throwA[StatusError]
    }

    "fail when trying to delete a non-existing document" in new freshDb {
      waitForResult(db.delete("foo", "bar")) must throwA[StatusError]
    }

    "fail when trying to delete a deleted document" in new freshDb {
      val (id, rev) = waitForResult(inserted(db.insert(JObject(Nil))))
      waitForResult(db.delete(id, rev))
      waitForResult(db.delete(id, rev)) must throwA[StatusError]
    }

    "fail when trying to delete an older revision of a document" in new freshDb {
      val (id, rev) = waitForResult(inserted(db.insert(Map("key" -> "value"))))
      val doc = waitForResult(db(id))
      waitForResult(db.insert(doc + ("key" -> "newValue")))
      waitForResult(db.delete(id, rev)) must throwA[StatusError]
    }
  }

  "db.bulkDocs" should {

    "be able to insert a single document" in new freshDb {
      (waitForResult(db.bulkDocs(Seq(Map("_id" -> "docid"))))(0) \ "id").extract[String] must be equalTo "docid"
    }

    "fail to insert a duplicate document" in new freshDb {
      waitForResult(db.bulkDocs(Seq(Map("_id" -> "docid"))))
      (waitForResult(db.bulkDocs(Seq(Map("_id" -> "docid", "extra" -> "other"))))(0) \ "error").extract[String] must be equalTo "conflict"
    }

    "fail to insert a duplicate document at once" in new freshDb {
      (waitForResult(db.bulkDocs(Seq(Map("_id" -> "docid"),
        Map("_id" -> "docid", "extra" -> "other"))))(1) \ "error").extract[String] must be equalTo "conflict"
    }

    "accept to insert a duplicate document in batch mode" in new freshDb {
      (waitForResult(db.bulkDocs(Seq(Map("_id" -> "docid"),
        Map("_id" -> "docid", "extra" -> "other")),
        true))(1) \ "id").extract[String] must be equalTo "docid"
    }

    "generate conflicts when inserting duplicate documents in batch mode" in new freshDb {
      waitForResult(db.bulkDocs(Seq(Map("_id" -> "docid"),
        Map("_id" -> "docid", "extra" -> "other"),
        Map("_id" -> "docid", "extra" -> "yetAnother")),
        true))
      (waitForResult(db("docid", Map("conflicts" -> "true"))) \ "_conflicts").children must have size 2
    }

  }

  "db.allDocs" should {

    "return an empty count for an empty database" in new freshDb {
      waitForResult(db.allDocs()).total_rows must be equalTo 0
    }

    "return a correct count when an element has been inserted" in new freshDb {
      waitForResult(db.insert(Map("key" -> "value"), "docid"))
      waitForResult(db.allDocs()).total_rows must be equalTo 1
    }

    "return a correct id enumeration when an element has been inserted" in new freshDb {
      waitForResult(db.insert(Map("key" -> "value"), "docid"))
      waitForResult(db.allDocs()).ids must be equalTo List("docid")
    }

    "return a correct key enumeration when an element has been inserted" in new freshDb {
      waitForResult(db.insert(Map("key" -> "value"), "docid"))
      waitForResult(db.allDocs()).keys[String] must be equalTo List("docid")
    }

    "return a correct values enumeration when an element has been inserted" in new freshDb {
      waitForResult(db.insert(Map("key" -> "value"), "docid"))
      val JString(rev) = waitForResult(db.allDocs()).values[JValue].head \ "rev"
      rev must be matching "1-[0-9a-f]{32}"
    }

    "be convertible to an items triple" in new freshDb {
      waitForResult(db.insert(Map("key" -> "value"), "docid"))
      val (id: String, key: String, value: JValue) = waitForResult(db.allDocs()).items[String, JValue].head
      (value \ "rev").extract[String] must be matching "1-[0-9a-f]{32}"
    }

    "be convertible to an items quartuple in include_docs mode" in new freshDb {
      waitForResult(db.insert(Map("key" -> "value"), "docid"))
      val (id: String, key: String, value: JValue, doc: JValue) =
        waitForResult(db.allDocs(Map("include_docs" -> "true"))).itemsWithDoc[String, JValue, JValue].head
      ((value \ "rev").extract[String] must be matching "1-[0-9a-f]{32}") &&
        ((value \ "rev") must be equalTo(doc \ "_rev")) &&
        ((doc \ "key").extract[String] must be equalTo "value")
    }

    "not return full docs in default mode" in new freshDb {
      waitForResult(db.insert(Map("key" -> "value"), "docid"))
      waitForResult(db.allDocs()).docsOption[JValue] must be equalTo List(None)
    }

    "return full docs in include_docs mode" in new freshDb {
      waitForResult(db.insert(Map("key" -> "value")))
      waitForResult(db.allDocs(Map("include_docs" -> "true"))).docs[JValue].head \ "key" must
        be equalTo JString("value")
    }

    "return full docs in include_docs mode and option" in new freshDb {
      waitForResult(db.insert(Map("key" -> "value"), "docid"))
      waitForResult(db.allDocs(Map("include_docs" -> "true"))).docsOption[JValue].head.map(_ \ "key") must
        be equalTo Some(JString("value"))
    }

  }

  "db.compact()" should {
    "return with success" in new freshDb {
      waitForResult(db.compact()) \ "ok" must be equalTo JBool(true)
    }
  }

  "db.ensureFullCommit()" should {
    "return with success" in new freshDb {
      waitForResult(db.ensureFullCommit()) \ "ok" must be equalTo JBool(true)
    }
  }

  "db.changes()" should {
    "represent an empty set" in new freshDb {
      (waitForResult(db.changes()) \ "results").extract[List[JObject]] must beEmpty
    }

    "contain the only change" in new freshDb {
      val id = waitForResult(insertedId(db.insert(Map())))
      (waitForResult(db.changes()) \ "results").extract[List[JObject]].head \ "id" must be equalTo JString(id)
    }

    "return the change in long-polling state" in new freshDb {
      val changes = db.changes(Map("feed" -> "longpoll"))
      changes.isCompleted must beFalse
      val id = waitForResult(insertedId(db.insert(Map())))
      (waitForResult(changes) \ "results").extract[List[JObject]].head \ "id" must be equalTo JString(id)
    }
  }

  "db.revs_limit()" should {
    "be settable and queryable" in new freshDb {
      waitForResult(db.revs_limit(1938))
      waitForResult(db.revs_limit()) must be equalTo 1938
    }
  }

  "db.continuousChanges()" should {
    "see the creation of new documents" in new freshDb {
      implicit val materializer = ActorFlowMaterializer(None)
      val changes: Source[JObject, Unit] = db.continuousChanges()
      val result = changes.map(j => (j \ "id").extract[String]).take(3).runFold[List[String]](Nil)(_ :+ _)
      db.insert(JObject(Nil), "docid1")
      db.insert(JObject(Nil), "docid2")
      db.insert(JObject(Nil), "docid3")
      waitForResult(result).sorted must be equalTo List("docid1", "docid2", "docid3")
    }

    "reconnect automatically" in new freshDb {
      implicit val materializer = ActorFlowMaterializer(None)
      val changes: Source[JObject, Unit] = db.continuousChanges(Map("timeout" -> "100"))
      val result = changes.map(j => (j \ "id").extract[String]).take(3).runFold[List[String]](Nil)(_ :+ _)
      db.insert(JObject(Nil), "docid1")
      Thread.sleep(200)
      db.insert(JObject(Nil), "docid2")
      db.insert(JObject(Nil), "docid3")
      waitForResult(result).sorted must be equalTo List("docid1", "docid2", "docid3")
    }

    "be able to filter changes" in new freshDb {
      implicit val materializer = ActorFlowMaterializer(None)
      val filter = """function(doc, req) { return doc.name == "foo"; }"""
      waitForResult(db.insert(Map("filters" -> Map("namedfoo" -> filter)), "_design/common"))
      val changes: Source[JObject, Unit] = db.continuousChanges(Map("filter" -> "common/namedfoo"))
      val result = changes.map(j => (j \ "id").extract[String]).take(2).runFold[List[String]](Nil)(_ :+ _)
      waitForResult(db.insert(Map("name" -> "foo"), "docid1"))
      waitForResult(db.insert(Map("name" -> "bar"), "docid2"))
      waitForResult(db.insert(Map("name" -> "foo"), "docid3"))
      waitForResult(db.insert(Map("name" -> "bar"), "docid4"))
      waitForResult(result).sorted must be equalTo List("docid1", "docid3")
    }
  }

  "db.update" should {

    def installUpdate(db: Database) = {
      val update = """
                     | function(doc, req) {
                     |   var newdoc = JSON.parse(req.form.json);
                     |   newdoc._id = req.id || req.uuid ;
                     |   if (doc && doc._rev)
                     |     newdoc._rev = doc._rev;
                     |   return [newdoc, {
                     |     headers : {
                     |      "Content-Type" : "application/json"
                     |      },
                     |     body: JSON.stringify(newdoc)
                     |   }];
                     | };
                   """.stripMargin
      waitForResult(db.insert(Map("updates" -> Map("upd" -> update)), "_design/common"))
    }

    "properly encode values" in new freshDb {
      installUpdate(db)
      val newDoc = waitForResult(db.update("common", "upd", "docid", Map("json" -> compactRender(Map("foo" -> "bar")))))
      newDoc \ "_id" must be equalTo JString("docid")
      newDoc \ "_rev" must be equalTo JNothing
      newDoc \ "foo" must be equalTo JString("bar")
    }

    "properly insert documents" in new freshDb {
      installUpdate(db)
      waitForResult(db.update("common", "upd", "docid", Map("json" -> compactRender(Map("foo" -> "bar")))))
      val newDoc = waitForResult(db("docid"))
      newDoc \ "_id" must be equalTo JString("docid")
      (newDoc \ "_rev").extract[String] must startWith("1-")
      newDoc \ "foo" must be equalTo JString("bar")
    }

    "properly update documents" in new freshDb {
      installUpdate(db)
      waitForResult(db.update("common", "upd", "docid", Map("json" -> compactRender(Map("foo" -> "bar")))))
      waitForResult(db.update("common", "upd", "docid", Map("json" -> compactRender(Map("foo2" -> "bar2")))))
      val updatedDoc = waitForResult(db("docid"))
      updatedDoc \ "_id" must be equalTo JString("docid")
      (updatedDoc \ "_rev").extract[String] must startWith("2-")
      updatedDoc \ "foo" must be equalTo JNothing
      updatedDoc \ "foo2" must be equalTo JString("bar2")
    }
  }

}
