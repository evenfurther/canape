import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.testkit.TestProbe
import net.rfc1149.canape.Couch.StatusError
import play.api.libs.json._

import scala.concurrent.Future
import scala.concurrent.duration._

class ChangesSourceSpec extends WithDbSpecification("db") {

  "db.changesSource()" should {

    "respect the since parameter" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      waitForResult(db.insert(JsObject(Nil), "docid0"))
      val changes: Source[JsObject, Future[Long]] = db.changesSource(initialSeq = 0)
      val result = changes.map(j => (j \ "id").as[String]).take(4).runFold[List[String]](Nil)(_ :+ _)
      waitEventually(db.insert(JsObject(Nil), "docid1"), db.insert(JsObject(Nil), "docid2"), db.insert(JsObject(Nil), "docid3"))
      waitForResult(result).sorted must be equalTo List("docid0", "docid1", "docid2", "docid3")
    }

    "obtain the initial since parameter" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      waitForResult(db.insert(JsObject(Nil), "docid0"))
      val changes: Source[JsObject, Future[Long]] = db.changesSource()
      val (initialSeq, result) = changes.map(j => (j \ "id").as[String]).take(3).toMat(Sink.fold[List[String], String](Nil)(_ :+ _))(Keep.both).run()
      waitForResult(initialSeq) must be equalTo 1
      waitEventually(db.insert(JsObject(Nil), "docid1"), db.insert(JsObject(Nil), "docid2"), db.insert(JsObject(Nil), "docid3"))
      waitForResult(result).sorted must be equalTo List("docid1", "docid2", "docid3")
    }

    "reconnect in case of a timeout" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      waitForResult(db.insert(JsObject(Nil), "docid0"))
      val changes: Source[JsObject, Future[Long]] = db.changesSource(initialSeq = 0, params = Map("timeout" -> "1"))
      val (initialSeq, result) = changes.map(j => (j \ "id").as[String]).take(4).toMat(Sink.fold[List[String], String](Nil)(_ :+ _))(Keep.both).run()
      waitForResult(initialSeq) must be equalTo 0
      waitEventually(db.insert(JsObject(Nil), "docid1"), db.insert(JsObject(Nil), "docid2"), db.insert(JsObject(Nil), "docid3"))
      waitForResult(result).sorted must be equalTo List("docid0", "docid1", "docid2", "docid3")
    }

    "terminate on error if the database is deleted" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      waitForResult(db.insert(JsObject(Nil), "docid0"))
      val changes: Source[JsObject, Future[Long]] = db.changesSource()
      val result = changes.runWith(Sink.ignore)
      waitForResult(db.insert(JsObject(Nil), "docid1"))
      waitForResult(db.insert(JsObject(Nil), "docid2"))
      waitForResult(db.insert(JsObject(Nil), "docid3"))
      waitForResult(db.delete())
      waitForResult(result) must throwA[StatusError]("404 no_db_file: not_found")
    }

    "return the existing documents before the error if the database is deleted" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      waitForResult(db.insert(JsObject(Nil), "docid0"))
      val changes: Source[JsObject, Future[Long]] = db.changesSource(initialSeq = 0).recoverWith { case _ => Source.empty }
      val result = changes.map(j => (j \ "id").as[String]).runFold[List[String]](Nil)(_ :+ _)
      waitForResult(db.insert(JsObject(Nil), "docid1"))
      waitForResult(db.insert(JsObject(Nil), "docid2"))
      waitForResult(db.insert(JsObject(Nil), "docid3"))
      waitForResult(db.delete())
      waitForResult(result).sorted must be equalTo List("docid0", "docid1", "docid2", "docid3")
    }

    "see the creation of new documents as soon as they are created" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      val probe = TestProbe()
      val changes: Source[JsObject, Future[Long]] = db.changesSource(initialSeq = 0)
      val result = changes.map(j => (j \ "id").as[String]).take(3).runWith(Sink.actorRef(probe.ref, "end"))
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
      val changes: Source[JsObject, Future[Long]] = db.changesSource(initialSeq = 0)
      val result = changes.map(j => (j \ "id").as[String]).take(3).runFold[List[String]](Nil)(_ :+ _)
      waitEventually(db.insert(JsObject(Nil), "docidé"), db.insert(JsObject(Nil), "docidà"), db.insert(JsObject(Nil), "docidß"))
      waitForResult(result).sorted must be equalTo List("docidß", "docidà", "docidé")
    }

    "be able to filter changes with a stored filter" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      val filter = """function(doc, req) { return doc.name == "foo"; }"""
      waitForEnd(db.insert(Json.obj("filters" -> Json.obj("namedfoo" -> filter)), "_design/common"))
      val changes: Source[JsObject, Future[Long]] = db.changesSource(initialSeq = 0, params = Map("filter" -> "common/namedfoo"))
      val result = changes.map(j => (j \ "id").as[String]).take(2).runFold[List[String]](Nil)(_ :+ _)
      waitEventually(db.bulkDocs(Seq(Json.obj("name" -> "foo", "_id" -> "docid1"), Json.obj("name" -> "bar", "_id" -> "docid2"),
        Json.obj("name" -> "foo", "_id" -> "docid3"), Json.obj("name" -> "bar", "_id" -> "docid4"))))
      waitForResult(result).sorted must be equalTo List("docid1", "docid3")
    }

    "be able to filter changes by document ids" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      val filter = """function(doc, req) { return doc.name == "foo"; }"""
      val changes: Source[JsObject, Future[Long]] = db.changesSourceByDocIds(List("docid1", "docid4"), initialSeq = 0)
      val result = changes.map(j => (j \ "id").as[String]).take(2).runFold[List[String]](Nil)(_ :+ _)
      waitEventually(db.bulkDocs(Seq(Json.obj("name" -> "foo", "_id" -> "docid1"), Json.obj("name" -> "bar", "_id" -> "docid2"),
        Json.obj("name" -> "foo", "_id" -> "docid3"), Json.obj("name" -> "bar", "_id" -> "docid4"))))
      waitForResult(result).sorted must be equalTo List("docid1", "docid4")
    }

    "fail properly if the database is absent" in new freshDb {
      implicit val materializer = ActorMaterializer(None)
      val newDb = db.couch.db("nonexistent-database")
      val result = newDb.changesSource().runFold[List[JsObject]](Nil)(_ :+ _)
      waitForResult(result) must throwA[StatusError]("404 no_db_file: not_found")
    }

  }

}
