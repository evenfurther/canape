import akka.NotUsed
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestProbe
import net.rfc1149.canape.Couch.StatusError
import net.rfc1149.canape._
import play.api.libs.json._

import scala.concurrent.duration._

class ContinuousChangesSpec extends WithDbSpecification("db") {

  "db.continuousChanges()" should {

    "see the creation of new documents" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      val changes: Source[JsObject, NotUsed] = db.continuousChanges()
      val result = changes.via(Database.onlySeq).map(j => (j \ "id").as[String]).take(3).runFold[List[String]](Nil)(_ :+ _)
      waitEventually(db.insert(JsObject(Nil), "docid1"), db.insert(JsObject(Nil), "docid2"), db.insert(JsObject(Nil), "docid3"))
      waitForResult(result).sorted must be equalTo List("docid1", "docid2", "docid3")
    }

    "see the creation of new documents as soon as they are created" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      val probe = TestProbe()
      val changes: Source[JsObject, NotUsed] = db.continuousChanges()
      val result = changes.via(Database.onlySeq).map(j => (j \ "id").as[String]).take(3).runWith(Sink.actorRef(probe.ref, "end"))
      waitEventually(db.insert(JsObject(Nil), "docid1"))
      probe.expectMsg(5.seconds, "docid1")
      waitEventually(db.insert(JsObject(Nil), "docid2"))
      probe.expectMsg(5.seconds, "docid2")
      waitEventually(db.insert(JsObject(Nil), "docid3"))
      probe.expectMsg(5.seconds, "docid3")
      probe.expectMsg(5.seconds, "end")
    }

    "see the creation of new documents with non-ASCII id" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      val changes: Source[JsObject, NotUsed] = db.continuousChanges()
      val result = changes.map(j => (j \ "id").as[String]).take(3).runFold[List[String]](Nil)(_ :+ _)
      waitEventually(db.insert(JsObject(Nil), "docidé"), db.insert(JsObject(Nil), "docidà"), db.insert(JsObject(Nil), "docidß"))
      waitForResult(result).sorted must be equalTo List("docidß", "docidà", "docidé")
    }

    "properly disconnect after a timeout" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      val changes: Source[JsObject, NotUsed] = db.continuousChanges(Map("timeout" -> "100"))
      val result = changes.map(_ \ "id").collect { case JsDefined(JsString(id)) => id }.runFold[List[String]](Nil)(_ :+ _)
      waitForResult(result).sorted must be equalTo List()
    }

    "see documents operations occuring before the timeout" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      waitForEnd(db.insert(JsObject(Nil), "docid1"), db.insert(JsObject(Nil), "docid2"))
      val changes: Source[JsObject, NotUsed] = db.continuousChanges(Map("timeout" -> "100"))
      val result = changes.map(_ \ "id").collect { case JsDefined(JsString(id)) => id }.runFold[List[String]](Nil)(_ :+ _)
      waitForResult(result).sorted must be equalTo List("docid1", "docid2")
    }

    "be able to filter changes with a stored filter" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      val filter = """function(doc, req) { return doc.name == "foo"; }"""
      waitForEnd(db.insert(Json.obj("filters" -> Json.obj("namedfoo" -> filter)), "_design/common"))
      val changes: Source[JsObject, NotUsed] = db.continuousChanges(Map("filter" -> "common/namedfoo"))
      val result = changes.map(j => (j \ "id").as[String]).take(2).runFold[List[String]](Nil)(_ :+ _)
      waitEventually(db.bulkDocs(Seq(Json.obj("name" -> "foo", "_id" -> "docid1"), Json.obj("name" -> "bar", "_id" -> "docid2"),
        Json.obj("name" -> "foo", "_id" -> "docid3"), Json.obj("name" -> "bar", "_id" -> "docid4"))))
      waitForResult(result).sorted must be equalTo List("docid1", "docid3")
    }

    "be able to filter changes by document ids" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      val filter = """function(doc, req) { return doc.name == "foo"; }"""
      val changes: Source[JsObject, NotUsed] = db.continuousChangesByDocIds(List("docid1", "docid4"))
      val result = changes.map(j => (j \ "id").as[String]).take(2).runFold[List[String]](Nil)(_ :+ _)
      waitEventually(db.bulkDocs(Seq(Json.obj("name" -> "foo", "_id" -> "docid1"), Json.obj("name" -> "bar", "_id" -> "docid2"),
        Json.obj("name" -> "foo", "_id" -> "docid3"), Json.obj("name" -> "bar", "_id" -> "docid4"))))
      waitForResult(result).sorted must be equalTo List("docid1", "docid4")
    }

    "fail properly if the database is absent" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      val newDb = db.couch.db("nonexistent-database")
      val result = newDb.continuousChanges().runFold[List[JsObject]](Nil)(_ :+ _)
      waitForResult(result) must throwA[StatusError]("404 no_db_file: not_found")
    }

    "fail properly if the HTTP server is not running" in {
      implicit val materializer = ActorMaterializer(None)
      val newDb = new Couch("localhost", 5985).db("not-running-anyway")
      val result = newDb.continuousChanges().runFold[List[JsObject]](Nil)(_ :+ _)
      waitForResult(result) must throwA[RuntimeException]
    }

    "fail properly if the HTTPS server is not running" in {
      implicit val materializer = ActorMaterializer(None)
      val newDb = new Couch("localhost", 5985, secure = true).db("not-running-anyway")
      val result = newDb.continuousChanges().runFold[List[JsObject]](Nil)(_ :+ _)
      waitForResult(result) must throwA[RuntimeException]
    }

    "terminate properly if the database is deleted during the request" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      val result = db.continuousChanges().runFold[List[JsObject]](Nil)(_ :+ _)
      waitForResult(db.insert(JsObject(Nil), "docid1"))
      waitForResult(db.insert(JsObject(Nil), "docid2"))
      waitForResult(db.insert(JsObject(Nil), "docid3"))
      waitForResult(db.delete())
      val changes = waitForResult(result)
      changes must haveSize(4)
      (changes.last \ "last_seq").as[Long] must beEqualTo(3)
    }
  }

}
