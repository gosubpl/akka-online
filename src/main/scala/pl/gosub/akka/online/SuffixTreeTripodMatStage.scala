package pl.gosub.akka.online

import akka.Done
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}
import com.abahgat.suffixtree.GeneralizedSuffixTree
import com.google.common.hash.{BloomFilter, Funnels}

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.immutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.util.Random

/**
  * A Y-shaped flow of elements that consequently has two inputs
  * and one output, arranged like this:
  *
  * {{{
  *        +--------+
  *  In1 ~>|        |
  *        | tripod |~> Out
  *  In2 ~>|        |
  *        +--------+
  * }}}
  */
final case class TripodShape[-In1, -In2, +Out](
  in1:  Inlet[In1 @uncheckedVariance],
  in2:  Inlet[In2 @uncheckedVariance],
  out: Outlet[Out @uncheckedVariance]) extends Shape {
    override val inlets: immutable.Seq[Inlet[_]] = List(in1, in2)
    override val outlets: immutable.Seq[Outlet[_]] = List(out)

    override def deepCopy(): TripodShape[In1, In2, Out] =
      TripodShape(in1.carbonCopy(), in2.carbonCopy(), out.carbonCopy())
    override def copyFromPorts(inlets: immutable.Seq[Inlet[_]], outlets: immutable.Seq[Outlet[_]]): Shape = {
      require(inlets.size == 2, s"proposed inlets [${inlets.mkString(", ")}] do not fit TripodShape")
      require(outlets.size == 1, s"proposed outlets [${outlets.mkString(", ")}] do not fit TripodShape")
      TripodShape(inlets(0), inlets(1), outlets(0))
    }
  def reversed: Shape = copyFromPorts(inlets.reverse, outlets.reverse)
}

object TripodShape {
  def of[In1, In2, Out](
  in1:  Inlet[In1 @uncheckedVariance],
  in2:  Inlet[In2 @uncheckedVariance],
  out: Outlet[Out @uncheckedVariance]): TripodShape[In1, In2, Out] =
    TripodShape(in1, in2, out)

}


class SuffixTreeTripodMatStage extends GraphStageWithMaterializedValue[TripodShape[String, String, List[Int]], Future[Done]] {

  // Stage syntax
  val stringsIn: Inlet[String] = Inlet("BloomFilterCrossMatStage.stringsIn")
  val searchesIn: Inlet[String] = Inlet("BloomFilterCrossMatStage.searchesIn")
  val matchesOut: Outlet[List[Int]] = Outlet("BloomFilterCrossMatStage.matchesOut")
  override val shape: TripodShape[String, String, List[Int]] = TripodShape(stringsIn, searchesIn, matchesOut)

  // Stage semantics
  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    // Completion notification
    val p: Promise[Done] = Promise()

    val logic = new GraphStageLogic(shape) {
      // State
      val sTree = new GeneralizedSuffixTree
      var index = 0

      // stringsIn effectively is a Sink,
      // so we need to kick it off
      override def preStart(): Unit = pull(stringsIn)

      setHandler(stringsIn, new InHandler {
        override def onPush(): Unit = {
          val elem = grab(stringsIn)
          println(s"Getting $elem")
          sTree.put(elem, index)
          index += 1
          pull(stringsIn)
        }

        override def onUpstreamFinish(): Unit = {
          completeStage()
        }
      })

      setHandler(searchesIn, new InHandler {
        override def onPush(): Unit = {
          val s = grab(searchesIn)
          import scala.collection.JavaConverters._
          val mi : Iterable[Integer] = sTree.search(s).asScala
          val m = mi.toList.map{_.toInt}
          if (isAvailable(matchesOut))
            push(matchesOut, m)
        }

        override def onUpstreamFinish(): Unit = {
          p.trySuccess(Done) // we are done when no more queries
          completeStage()
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          p.tryFailure(ex)
          failStage(ex)
        }
      })

      setHandler(matchesOut, new OutHandler {
        override def onPull(): Unit = {
          if (!hasBeenPulled(searchesIn))
            pull(searchesIn)
        }
      })

    }
    (logic, p.future)
  }
}

object SuffixTreeTripodMatStageMatMain extends App {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  val tripodStage = new SuffixTreeTripodMatStage

  val graph = RunnableGraph.fromGraph(GraphDSL.create(Sink.foreach(println)) { implicit builder => outMatches =>
    import GraphDSL.Implicits._
    val inStrings = Source.repeat(1).take(100).map(_ => "aaa").throttle(1, Duration(100, "millisecond"), 1, ThrottleMode.shaping)
    val inSearches = Source.repeat(1).take(10).map(_ => "aaa").throttle(1, Duration(1500, "millisecond"), 1, ThrottleMode.shaping)

    val tripod = builder.add(tripodStage)

    inStrings ~> tripod.in1; tripod.out ~> outMatches
    inSearches ~> tripod.in2
    ClosedShape
  }).run()

  import scala.concurrent.ExecutionContext.Implicits.global

  graph.onComplete(_ => system.terminate())
  Await.ready(system.whenTerminated, Duration.Inf)
}
