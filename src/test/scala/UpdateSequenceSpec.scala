import org.specs2.mutable._
import net.rfc1149.canape.Database._
import play.api.libs.json.Json

class UpdateSequenceSpec extends Specification {

  "UpdateSequence" should {

    "get the correct string value for FromNow" in {
      FromNow.asString must be equalTo "now"
    }

    "get the correct string value for FromStart" in {
      FromStart.asString must be equalTo "0"
    }

    "be parsable from a string" in {
      val js = Json.parse("""{"seq": "42-abcde"}""")
      val us = (js \ "seq").as[UpdateSequence]
      us.asString must be equalTo "42-abcde"
      us.asLong must be equalTo 42
    }

    "be parsable from a number" in {
      val js = Json.parse("""{"seq": 42}""")
      val us = (js \ "seq").as[UpdateSequence]
      us.asString must be equalTo "42"
      us.asLong must be equalTo 42
    }
  }

}
