import akka.http.scaladsl.model.HttpResponse
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import net.rfc1149.canape.Couch.StatusError
import net.rfc1149.canape._
import play.api.libs.json._

import scala.concurrent.Future
import scala.concurrent.duration._

class DatabaseSpec extends WithDbSpecification("db") {

  private def insertedId(f: Future[JsValue]): Future[String] = f map { js => (js \ "id").as[String] }

  private def insertedRev(f: Future[JsValue]): Future[String] = f map { js => (js \ "rev").as[String] }

  private def inserted(f: Future[JsValue]): Future[(String, String)] =
    for (id <- insertedId(f); rev <- insertedRev(f)) yield (id, rev)

  private def installDesignAndDocs(db: Database) = {
    val upd =
      """
        | function(doc, req) {
        |   var newdoc = JSON.parse(req.form.json);
        |   newdoc._id = req.id || req.uuid;
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
    val personsMap =
      """
        | function(doc) {
        |   if (doc.type == "person") {
        |     emit(doc.firstName, { "name": doc.firstName, "age": doc.age });
        |     emit(doc.lastName, { "name": doc.lastName, "age": doc.age });
        |   }
        | }
      """.stripMargin
    val personsReduce =
      """
        | function(key, values, rereduce) {
        |   return Math.max.apply(Math, rereduce ? values : values.map(function(p) { return p.age; }));
        | }
      """.stripMargin
    val list =
      """
        | function(head, req) {
        |   var first = true
        |   while (row = getRow()) {
        |     if (!first) {
        |       send(",");
        |     }
        |     first = false;
        |     send(row.value);
        |   }
        | }
      """.stripMargin
    val common = Json.obj("updates" -> Json.obj("upd" -> upd),
      "views" -> Json.obj("persons" -> Json.obj("map" -> personsMap, "reduce" -> personsReduce)),
      "lists" -> Json.obj("list" -> list))
    waitForResult(db.insert(common, "_design/common"))
    waitForResult(Future.sequence(for ((f, l, a) <- List(("Arthur", "Dent", 20), ("Zaphod", "Beeblebrox", 40),
      ("Buffy", "Summers", 23), ("Arthur", "Fubar", 27)))
      yield db.insert(Json.obj("firstName" -> f, "lastName" -> l, "age" -> a, "type" -> "person"))))
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

  "db.insert()" should {

    "be able to insert a new document with an explicit id" in new freshDb {
      waitForResult(insertedId(db.insert(JsObject(Nil), "docid"))) must be equalTo "docid"
    }

    "be able to insert a new document with an explicit non-ASCII id" in new freshDb {
      waitForResult(insertedId(db.insert(JsObject(Nil), "docéçà"))) must be equalTo "docéçà"
    }

    "be able to insert a new document with an explicit id in batch mode" in new freshDb {
      waitForResult(insertedId(db.insert(JsObject(Nil), "docid", batch = true))) must be equalTo "docid"
    }

    "be able to insert a new document with an implicit id" in new freshDb {
      waitForResult(insertedId(db.insert(JsObject(Nil)))) must be matching "[0-9a-f]{32}"
    }

    "be able to insert a new document with an implicit id in batch mode" in new freshDb {
      waitForResult(insertedId(db.insert(JsObject(Nil), batch = true))) must be matching "[0-9a-f]{32}"
    }

    "be able to insert a new document with an embedded id" in new freshDb {
      waitForResult(insertedId(db.insert(Json.obj("_id" -> "docid")))) must be equalTo "docid"
    }

    "be able to insert a new document with an embedded id in batch mode" in new freshDb {
      waitForResult(insertedId(db.insert(Json.obj("_id" -> "docid"), batch = true))) must be equalTo "docid"
    }

    "be able to update a document with an embedded id" in new freshDb {
      waitForResult(insertedId(db.insert(Json.obj("_id" -> "docid")))) must be equalTo "docid"
      val updatedDoc = waitForResult(db("docid")) ++ Json.obj("foo" -> "bar")
      waitForResult(insertedId(db.insert(updatedDoc))) must be equalTo "docid"
    }

    "be able to update a document with an embedded id in batch mode" in new freshDb {
      waitForResult(insertedId(db.insert(Json.obj("_id" -> "docid")))) must be equalTo "docid"
      val updatedDoc = waitForResult(db("docid")) ++ Json.obj("foo" -> "bar")
      waitForResult(insertedId(db.insert(updatedDoc, batch = true))) must be equalTo "docid"
    }

    "return a consistent rev" in new freshDb {
      waitForResult(insertedRev(db.insert(JsObject(Nil), "docid"))) must be matching "1-[0-9a-f]{32}"
    }

  }

  "db.apply()" should {

    "be able to retrieve the content of a document" in new freshDb {
      val id = waitForResult(insertedId(db.insert(Json.obj("key" -> "value"))))
      val doc = waitForResult(db(id))
      (doc \ "key").as[String] must be equalTo "value"
    }

    "be able to retrieve an older revision of a document with two params" in new freshDb {
      val (id, rev) = waitForResult(inserted(db.insert(Json.obj("key" -> "value"))))
      val doc = waitForResult(db(id))
      waitForResult(db.insert(doc ++ Json.obj("key" -> "newValue")))
      (waitForResult(db(id, rev)) \ "key").as[String] must be equalTo "value"
    }

    "be able to retrieve an older revision of a document with a params map" in new freshDb {
      val (id, rev) = waitForResult(inserted(db.insert(Json.obj("key" -> "value"))))
      val doc = waitForResult(db(id))
      waitForResult(db.insert(doc ++ Json.obj("key" -> "newValue")))
      (waitForResult(db(id, Map("rev" -> rev))) \ "key").as[String] must be equalTo "value"
    }

    "be able to retrieve an older revision of a document with a params sequence" in new freshDb {
      val (id, rev) = waitForResult(inserted(db.insert(Json.obj("key" -> "value"))))
      val doc = waitForResult(db(id))
      waitForResult(db.insert(doc ++ Json.obj("key" -> "newValue")))
      (waitForResult(db(id, Seq("rev" -> rev))) \ "key").as[String] must be equalTo "value"
    }

    "be able to cope with documents containing non-ASCII characters" in new freshDb {
      private val s = "Épisode àçõœæéèêß"
      val id = waitForResult(insertedId(db.insert(Json.obj("data" -> s))))
      (waitForResult(db(id)) \ "data").as[String] must be equalTo s
    }

  }

  "db.delete()" should {

    "be able to delete a document" in new freshDb {
      val (id, rev) = waitForResult(inserted(db.insert(JsObject(Nil))))
      waitForResult(db.delete(id, rev))
      waitForResult(db(id)) must throwA[StatusError]
    }

    "fail when trying to delete a non-existing document" in new freshDb {
      waitForResult(db.delete("foo", "bar")) must throwA[StatusError]
    }

    "fail when trying to delete a deleted document" in new freshDb {
      val (id, rev) = waitForResult(inserted(db.insert(JsObject(Nil))))
      waitForResult(db.delete(id, rev))
      waitForResult(db.delete(id, rev)) must throwA[StatusError]
    }

    "fail when trying to delete an older revision of a document" in new freshDb {
      val (id, rev) = waitForResult(inserted(db.insert(Json.obj("key" -> "value"))))
      val doc = waitForResult(db(id))
      waitForResult(db.insert(doc ++ Json.obj("key" -> "newValue")))
      waitForResult(db.delete(id, rev)) must throwA[StatusError]
    }

    "be able to bulk delete zero revisions of a non-existent document" in new freshDb {
      waitForResult(db.delete("docid", Seq())) must beEmpty
    }

    "be able to bulk delete one revision of a document" in new freshDb {
      val (id, rev) = waitForResult(inserted(db.insert(Json.obj())))
      waitForResult(db.delete(id, Seq(rev))) must be equalTo Seq(rev)
      waitForResult(db(id)) must throwA[StatusError]
    }

    "be able to not fail at bulk deleting a unique non-existing revision of a document" in new freshDb {
      val (id, rev) = waitForResult(inserted(db.insert(Json.obj())))
      waitForResult(db.delete(id, Seq(rev.dropRight(8) + "00000000"))) must beEmpty
      (waitForResult(db(id)) \ "_rev").as[String] must be equalTo rev
    }

    "be able to not fail at bulk deleting a unique revision of a non-existing document" in new freshDb {
      val (id, rev) = waitForResult(inserted(db.insert(Json.obj())))
      waitForResult(db.delete("docid", Seq(rev.dropRight(8) + "00000000"))) must beEmpty
    }

    "be able to delete selected revisions of a document" in new freshDb {
      val revs = waitForResult(db.bulkDocs(List("foo", "bar", "baz").map(v => Json.obj("_id" -> "docid", "value" -> v)),
        allOrNothing = true)).map(doc => (doc \ "rev").as[String])
      waitForResult(db.delete("docid", revs.drop(1))) must have size 2
      waitForResult(db.delete("docid", revs.take(1))) must have size 1
      waitForResult(db.delete("docid", revs.take(1))) must beEmpty
    }

    "be able to delete a document given only its id" in new freshDb {
      val (id, rev) = waitForResult(inserted(db.insert(Json.obj("key" -> "value"))))
      waitForResult(db.delete(id)) must be equalTo Seq(rev)
      waitForResult(db.delete(id)) must throwA[StatusError]
    }

    "be able to delete multiple revisions of a document given only its id" in new freshDb {
      waitForResult(db.bulkDocs(List("foo", "bar", "baz").map(v => Json.obj("_id" -> "docid", "value" -> v)),
        allOrNothing = true))
      waitForResult(db.delete("docid")) must have size 3
      waitForResult(db.delete("docid")) must throwA[StatusError]
    }
  }

  "db.bulkDocs" should {

    "be able to insert a single document" in new freshDb {
      (waitForResult(db.bulkDocs(Seq(Json.obj("_id" -> "docid")))).head \ "id").as[String] must be equalTo "docid"
    }

    "fail to insert a duplicate document" in new freshDb {
      waitForResult(db.bulkDocs(Seq(Json.obj("_id" -> "docid"))))
      (waitForResult(db.bulkDocs(Seq(Json.obj("_id" -> "docid", "extra" -> "other")))).head \ "error").as[String] must be equalTo "conflict"
    }

    "fail to insert a duplicate document at once" in new freshDb {
      (waitForResult(db.bulkDocs(Seq(Json.obj("_id" -> "docid"),
        Json.obj("_id" -> "docid", "extra" -> "other"))))(1) \ "error").as[String] must be equalTo "conflict"
    }

    "accept to insert a duplicate document in batch mode" in new freshDb {
      (waitForResult(db.bulkDocs(Seq(Json.obj("_id" -> "docid"),
        Json.obj("_id" -> "docid", "extra" -> "other")),
        allOrNothing = true))(1) \ "id").as[String] must be equalTo "docid"
    }

    "generate conflicts when inserting duplicate documents in batch mode" in new freshDb {
      waitForResult(db.bulkDocs(Seq(Json.obj("_id" -> "docid"),
        Json.obj("_id" -> "docid", "extra" -> "other"),
        Json.obj("_id" -> "docid", "extra" -> "yetAnother")),
        allOrNothing = true))
      (waitForResult(db("docid", Map("conflicts" -> "true"))) \ "_conflicts").as[Array[JsValue]] must have size 2
    }

  }

  "db.allDocs" should {

    "return an empty count for an empty database" in new freshDb {
      waitForResult(db.allDocs()).total_rows must be equalTo 0
    }

    "return a correct count when an element has been inserted" in new freshDb {
      waitForResult(db.insert(Json.obj("key" -> "value"), "docid"))
      waitForResult(db.allDocs()).total_rows must be equalTo 1
    }

    "return a correct id enumeration when an element has been inserted" in new freshDb {
      waitForResult(db.insert(Json.obj("key" -> "value"), "docid"))
      waitForResult(db.allDocs()).ids must be equalTo List("docid")
    }

    "return a correct key enumeration when an element has been inserted" in new freshDb {
      waitForResult(db.insert(Json.obj("key" -> "value"), "docid"))
      waitForResult(db.allDocs()).keys[String] must be equalTo List("docid")
    }

    "return a correct values enumeration when an element has been inserted" in new freshDb {
      waitForResult(db.insert(Json.obj("key" -> "value"), "docid"))
      val rev = (waitForResult(db.allDocs()).values[JsValue].head \ "rev").as[String]
      rev must be matching "1-[0-9a-f]{32}"
    }

    "be convertible to an items triple" in new freshDb {
      waitForResult(db.insert(Json.obj("key" -> "value"), "docid"))
      val (id: String, key: String, value: JsValue) = waitForResult(db.allDocs()).items[String, JsValue].head
      (value \ "rev").as[String] must be matching "1-[0-9a-f]{32}"
    }

    "be convertible to an items quartuple in include_docs mode" in new freshDb {
      waitForResult(db.insert(Json.obj("key" -> "value"), "docid"))
      val (id: String, key: String, value: JsValue, doc: Some[JsValue]) =
        waitForResult(db.allDocs(Map("include_docs" -> "true"))).itemsWithDoc[String, JsValue, JsValue].head
      ((value \ "rev").as[String] must be matching "1-[0-9a-f]{32}") &&
        ((value \ "rev") must be equalTo(doc.get \ "_rev")) &&
        ((doc.get \ "key").as[String] must be equalTo "value")
    }

    "not return full docs in default mode" in new freshDb {
      waitForResult(db.insert(Json.obj("key" -> "value"), "docid"))
      waitForResult(db.allDocs()).docs[JsValue] must be equalTo Nil
    }

    "return full docs in include_docs mode" in new freshDb {
      waitForResult(db.insert(Json.obj("key" -> "value")))
      (waitForResult(db.allDocs(Map("include_docs" -> "true"))).docs[JsValue].head \ "key").as[String] must be equalTo "value"
    }

    "return full docs in include_docs mode and option" in new freshDb {
      waitForResult(db.insert(Json.obj("key" -> "value"), "docid"))
      (waitForResult(db.allDocs(Map("include_docs" -> "true"))).docs[JsValue].head \ "key").as[String] must be equalTo "value"
    }

  }

  "db.compact()" should {
    "return with success" in new freshDb {
      (waitForResult(db.compact()) \ "ok").as[Boolean] must beTrue
    }
  }

  "db.ensureFullCommit()" should {
    "return with success" in new freshDb {
      (waitForResult(db.ensureFullCommit()) \ "ok").as[Boolean] must beTrue
    }
  }

  "db.changes()" should {
    "represent an empty set" in new freshDb {
      (waitForResult(db.changes()) \ "results").as[List[JsObject]] must beEmpty
    }

    "contain the only change" in new freshDb {
      val id = waitForResult(insertedId(db.insert(Json.obj())))
      ((waitForResult(db.changes()) \ "results").as[List[JsObject]].head \ "id").as[String] must be equalTo id
    }

    "return the change in long-polling state" in new freshDb {
      val changes = db.changes(Map("feed" -> "longpoll"))
      changes.isCompleted must beFalse
      val id = waitForResult(insertedId(db.insert(Json.obj())))
      ((waitForResult(changes) \ "results").as[List[JsObject]].head \ "id").as[String] must be equalTo id
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
      implicit val materializer = ActorMaterializer(None)
      val changes: Source[JsObject, Unit] = db.continuousChanges()
      val result = changes.via(Database.onlySeq).map(j => (j \ "id").as[String]).take(3).runFold[List[String]](Nil)(_ :+ _)
      waitEventually(db.insert(JsObject(Nil), "docid1"), db.insert(JsObject(Nil), "docid2"), db.insert(JsObject(Nil), "docid3"))
      waitForResult(result).sorted must be equalTo List("docid1", "docid2", "docid3")
    }

    "see the creation of new documents with non-ASCII id" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      val changes: Source[JsObject, Unit] = db.continuousChanges()
      val result = changes.map(j => (j \ "id").as[String]).take(3).runFold[List[String]](Nil)(_ :+ _)
      waitEventually(db.insert(JsObject(Nil), "docidé"), db.insert(JsObject(Nil), "docidà"), db.insert(JsObject(Nil), "docidß"))
      waitForResult(result).sorted must be equalTo List("docidß", "docidà", "docidé")
    }

    "properly disconnect after a timeout" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      val changes: Source[JsObject, Unit] = db.continuousChanges(Map("timeout" -> "100"))
      val result = changes.map(_ \ "id").collect { case JsDefined(JsString(id)) => id }.runFold[List[String]](Nil)(_ :+ _)
      waitForResult(result).sorted must be equalTo List()
    }

    "see documents operations occuring before the timeout" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      waitForEnd(db.insert(JsObject(Nil), "docid1"), db.insert(JsObject(Nil), "docid2"))
      val changes: Source[JsObject, Unit] = db.continuousChanges(Map("timeout" -> "100"))
      val result = changes.map(_ \ "id").collect { case JsDefined(JsString(id)) => id }.runFold[List[String]](Nil)(_ :+ _)
      waitForResult(result).sorted must be equalTo List("docid1", "docid2")
    }

    "be able to filter changes with a stored filter" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      val filter = """function(doc, req) { return doc.name == "foo"; }"""
      waitForEnd(db.insert(Json.obj("filters" -> Json.obj("namedfoo" -> filter)), "_design/common"))
      val changes: Source[JsObject, Unit] = db.continuousChanges(Map("filter" -> "common/namedfoo"))
      val result = changes.map(j => (j \ "id").as[String]).take(2).runFold[List[String]](Nil)(_ :+ _)
      waitEventually(db.bulkDocs(Seq(Json.obj("name" -> "foo", "_id" -> "docid1"), Json.obj("name" -> "bar", "_id" -> "docid2"),
        Json.obj("name" -> "foo", "_id" -> "docid3"), Json.obj("name" -> "bar", "_id" -> "docid4"))))
      waitForResult(result).sorted must be equalTo List("docid1", "docid3")
    }

    "be able to filter changes by document ids" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      val filter = """function(doc, req) { return doc.name == "foo"; }"""
      val changes: Source[JsObject, Unit] = db.continuousChangesByDocIds(List("docid1", "docid4"))
      val result = changes.map(j => (j \ "id").as[String]).take(2).runFold[List[String]](Nil)(_ :+ _)
      waitEventually(db.bulkDocs(Seq(Json.obj("name" -> "foo", "_id" -> "docid1"), Json.obj("name" -> "bar", "_id" -> "docid2"),
        Json.obj("name" -> "foo", "_id" -> "docid3"), Json.obj("name" -> "bar", "_id" -> "docid4"))))
      waitForResult(result).sorted must be equalTo List("docid1", "docid4")
    }

    "fail properly if the database is absent" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      val newDb = db.couch.db("nonexistent-database")
      val result = newDb.continuousChanges().runFold[List[JsObject]](Nil)(_ :+ _)
      waitForResult(result) must throwA[RuntimeException]("Not Found")
    }

    "fail properly if the server is not running" in {
      implicit val materializer = ActorMaterializer(None)
      val newDb = new Couch("localhost", 5985).db("not-running-anyway")
      val result = newDb.continuousChanges().runFold[List[JsObject]](Nil)(_ :+ _)
      waitForResult(result) must throwA[akka.stream.StreamTcpException]("Connection failed.")
    }
  }

  "db.update" should {

    "properly encode values" in new freshDb {
      installDesignAndDocs(db)
      val newDoc = waitForResult(db.update("common", "upd", "docid", Map("json" -> Json.stringify(Json.obj("foo" -> "bar")))))
      (newDoc \ "_id").as[String] must be equalTo "docid"
      (newDoc \ "_rev").toOption must beNone
      (newDoc \ "foo").as[String] must be equalTo "bar"
    }

    "properly encode values with non-ASCII characters" in new freshDb {
      installDesignAndDocs(db)
      val newDoc = waitForResult(db.update("common", "upd", "docid", Map("json" -> Json.stringify(Json.obj("foo" -> "barré")))))
      (newDoc \ "_id").as[String] must be equalTo "docid"
      (newDoc \ "_rev").toOption must beNone
      (newDoc \ "foo").as[String] must be equalTo "barré"
    }

    "properly insert documents" in new freshDb {
      installDesignAndDocs(db)
      waitForResult(db.update("common", "upd", "docid", Map("json" -> Json.stringify(Json.obj("foo" -> "bar")))))
      val newDoc = waitForResult(db("docid"))
      (newDoc \ "_id").as[String] must be equalTo "docid"
      (newDoc \ "_rev").as[String] must startWith("1-")
      (newDoc \ "foo").as[String] must be equalTo "bar"
    }

    "properly insert documents with non-ASCII characters in id" in new freshDb {
      installDesignAndDocs(db)
      waitForResult(db.update("common", "upd", "docidé", Map("json" -> Json.stringify(Json.obj("foo" -> "bar")))))
      val newDoc = waitForResult(db("docidé"))
      (newDoc \ "_id").as[String] must be equalTo "docidé"
      (newDoc \ "_rev").as[String] must startWith("1-")
      (newDoc \ "foo").as[String] must be equalTo "bar"
    }

    "properly update documents" in new freshDb {
      installDesignAndDocs(db)
      waitForResult(db.update("common", "upd", "docid", Map("json" -> Json.stringify(Json.obj("foo" -> "bar")))))
      waitForResult(db.update("common", "upd", "docid", Map("json" -> Json.stringify(Json.obj("foo2" -> "bar2")))))
      val updatedDoc = waitForResult(db("docid"))
      (updatedDoc \ "_id").as[String] must be equalTo "docid"
      (updatedDoc \ "_rev").as[String] must startWith("2-")
      (updatedDoc \ "foo").toOption must beNone
      (updatedDoc \ "foo2").as[String] must be equalTo "bar2"
    }
  }

  "db.mapOnly" should {

    "return correct values" in new freshDb {
      installDesignAndDocs(db)
      val result = waitForResult(db.mapOnly("common", "persons"))
      result.total_rows must be equalTo 8
    }
  }

  "db.view" should {

    "return correct values when not grouping" in new freshDb {
      installDesignAndDocs(db)
      val result = waitForResult(db.view[JsValue, Int]("common", "persons"))
      result.size must be equalTo 1
      result.head._1 must be equalTo JsNull
      result.head._2 must be equalTo 40
    }

    "return correct values when grouping" in new freshDb {
      installDesignAndDocs(db)
      val result = waitForResult(db.view[String, Int]("common", "persons", Seq("group" -> "true"))).toMap
      result.size must be equalTo 7
      result.keys must containAllOf(List("Arthur", "Beeblebrox", "Buffy", "Dent", "Fubar", "Summers", "Zaphod"))
      result.get("Arthur") must be equalTo Some(27)
    }

    "work as db.mapOnly when explicitely not reducing" in new freshDb {
      installDesignAndDocs(db)
      val result = waitForResult(db.view[JsValue, JsValue]("common", "persons", Seq("reduce" -> "false")))
      result.size must be equalTo 8
    }
  }

  "db.list" should {

    implicit val materializer = ActorMaterializer(None)

    def responseToString(response: HttpResponse): Future[String] =
      response.entity.toStrict(FiniteDuration(1, SECONDS)).map(s => new String(s.data.toArray, "UTF-8"))

    "return correct values when not grouping" in new freshDb {
      installDesignAndDocs(db)
      val result = waitForResult(db.list("common", "list", "persons").flatMap(responseToString))
      result must be equalTo "40"
    }

    "return correct values when grouping" in new freshDb {
      installDesignAndDocs(db)
      val result = waitForResult(db.list("common", "list", "persons", Seq("group" -> "true")).flatMap(responseToString)).split(',').map(_.toInt).sorted
      result must be equalTo Array(20, 23, 23, 27, 27, 40, 40)
    }
  }

}
