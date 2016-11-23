package pl.gosub.akka.online

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Random

/*
def max_subarray(A):
max_ending_here = max_so_far = 0
for x in A:
    max_ending_here = max(0, max_ending_here + x)
    max_so_far = max(max_so_far, max_ending_here)
return max_so_far
 */

class KadaneFlowStage extends GraphStage[FlowShape[Int, Int]] {

  /*
  This stage has a Flow shape
              +-------+
              |       |
  ---> >Inlet > Logic > Outlet> --->
              |       |
              +-------+
   */

  // Shape definition
  val in: Inlet[Int] = Inlet("KadaneFlowStage.in")
  val out: Outlet[Int] = Outlet("KadaneFlowStage.out")
  override val shape: FlowShape[Int, Int] = FlowShape(in, out)

  // Logic for the stage
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      // state
      var maxEndingHere = 0
      var maxSoFar = 0

      // Handler(s) for the Inlet
      setHandler(in, new InHandler {
        // what to do when a new element is ready to be consumed
        override def onPush(): Unit = {
          val elem = grab(in)

          // "Business" logic
          maxEndingHere = Math.max(0, maxEndingHere + elem)
          maxSoFar = Math.max(maxSoFar, maxEndingHere)

          // this should never happen
          // we decide to not push the value, avoiding the error
          // but potentially losing the value
          if (isAvailable(out))
            push(out, maxSoFar)
        }

        override def onUpstreamFinish(): Unit = {
          completeStage()
        }
      })

      // Handler for the Outlet
      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          if (!hasBeenPulled(in))
            pull(in)
        }
      })

    }

}

object KadaneFlowMain extends App {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  val kadaneFlowStage = new KadaneFlowStage

  val done = Source.repeat(1).take(100).map(_ => Random.nextInt(1100) - 1000)
//    .throttle(1, Duration(100, "millisecond"), 1, ThrottleMode.shaping)
    .via(kadaneFlowStage)
    .throttle(1, Duration(100, "millisecond"), 1, ThrottleMode.shaping)
    .runWith(Sink.foreach(println))

  import scala.concurrent.ExecutionContext.Implicits.global

  done.onComplete(_ => system.terminate())
  Await.ready(system.whenTerminated, Duration.Inf)
}
