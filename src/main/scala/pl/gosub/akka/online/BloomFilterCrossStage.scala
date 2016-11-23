package pl.gosub.akka.online

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.google.common.hash.{BloomFilter, Funnels}

import scala.concurrent.duration.Duration
import scala.util.Random

// Cross Shape is actually BidiShape - for shape the semantics doesn't count, only syntax
class BloomFilterCrossStage extends GraphStage[BidiShape[Int, Int, Int, String]] {

  // Stage syntax
  val dataIn: Inlet[Int] = Inlet("BloomFilterCrossStage.dataIn")
  val dataOut: Outlet[Int] = Outlet("BloomFilterCrossStage.dataOut")
  val queriesIn: Inlet[Int] = Inlet("BloomFilterCrossStage.queriesIn")
  val answersOut: Outlet[String] = Outlet("BloomFilterCrossStage.answersOut")
  override val shape: BidiShape[Int, Int, Int, String] = BidiShape(dataIn, dataOut, queriesIn, answersOut)

  // Stage semantics
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      // State
      var maxEndingHere = 0
      var maxSoFar = 0
      val filter = BloomFilter.create[Integer](Funnels.integerFunnel(), 1000, 0.01)

      setHandler(dataIn, new InHandler {
        override def onPush(): Unit = {
          val elem = grab(dataIn)
          filter.put(elem)
          if (isAvailable(dataOut))
            push(dataOut, elem)
        }

        override def onUpstreamFinish(): Unit = {
          completeStage()
        }
      })

      setHandler(dataOut, new OutHandler {
        override def onPull(): Unit = {
          if (!hasBeenPulled(dataIn))
            pull(dataIn)
        }
      })

      setHandler(queriesIn, new InHandler {
        override def onPush(): Unit = {
          val x = grab(queriesIn)
          val answer = if (filter.mightContain(x)) {
            s"MAYBE, filter probably contains $x"
          } else {
            s"NO, filter definitely does not contain $x"
          }
          if (isAvailable(answersOut))
            push(answersOut, answer)
        }

        override def onUpstreamFinish(): Unit = {
          completeStage()
        }
      })

      setHandler(answersOut, new OutHandler {
        override def onPull(): Unit = {
          if (!hasBeenPulled(queriesIn))
            pull(queriesIn)
        }
      })

    }

}

object BloomFilterCrossStageMain extends App {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  val crossStage = new BloomFilterCrossStage

  val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
    import GraphDSL.Implicits._
    val inData = Source.repeat(1).take(1000).map(_ => Random.nextInt(1000)).throttle(1, Duration(100, "millisecond"), 1, ThrottleMode.shaping)
    val outData = Sink.foreach(println)
    val inControl = Source.repeat(1).take(100).map(_ => Random.nextInt(2000) - 1000).throttle(1, Duration(1500, "millisecond"), 1, ThrottleMode.shaping)
    val outControl = Sink.foreach(println)

    val cross = builder.add(crossStage)

    inData ~> cross.in1; cross.out1 ~> outData
    inControl ~> cross.in2; cross.out2 ~> outControl
    ClosedShape
  }).run()
}
