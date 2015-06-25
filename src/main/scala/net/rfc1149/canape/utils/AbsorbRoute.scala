package net.rfc1149.canape
package utils

import akka.stream.FanOutShape.{Init, Name}
import akka.stream.scaladsl.{FlexiRoute, Flow, FlowGraph, Sink}
import akka.stream.{Attributes, FanOutShape}

case class AbsorbRouteShape[A](_init: Init[A] = Name[A]("Absorb")) extends FanOutShape[A](_init) {
  val out = newOutlet[A]("out")
  val dummy = newOutlet[A]("dummy")
  protected override def construct(i: Init[A]) = new AbsorbRouteShape(i)
  }

class AbsorbRoute[A] extends FlexiRoute[A, AbsorbRouteShape[A]](AbsorbRouteShape(), Attributes.name("Absorb")) {

  import FlexiRoute._

  override def createRouteLogic(p: PortT) = new RouteLogic[A] {
    override def initialState =
      State[Any](DemandFrom(p.out)) {
        (ctx, _, element) =>
          println(s"Emitting element in regular state: $element")
          ctx.emit(p.out)(element)
          SameState
      }

    private val dummyState = State[Any](DemandFrom(p.dummy)) {
      (ctx, _, element) =>
        println(s"Emitting element in dummy state: $element")
        SameState
    }

    override def initialCompletionHandling = CompletionHandling(
      onUpstreamFinish = (ctx) => (),
      onUpstreamFailure = (ctx, thr) => (),
      onDownstreamFinish = (ctx, output) => {
        println("Switching to dummy state")
        dummyState
      }
    )
  }

}

object Absorb {
  def absorber[A] = Flow() { implicit b =>
    import FlowGraph.Implicits._

    val absorber = b.add(new AbsorbRoute[A])
    absorber.dummy ~> b.add(Sink.ignore)

    (absorber.in, absorber.out)
  }
}
