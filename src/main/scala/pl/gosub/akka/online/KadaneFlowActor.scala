package pl.gosub.akka.online

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import akka.event.Logging
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult, ThrottleMode}
import akka.stream.scaladsl.{Sink, Source, SourceQueueWithComplete}
import akka.pattern.pipe

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Random // For the pipeTo pattern

class KadaneFlowActor(queue: SourceQueueWithComplete[Int]) extends Actor {
  val log = Logging(context.system, this)
  // state
  var max_ending_here = 0
  var max_so_far = 0
  var upStream : ActorRef = self

  // Execution context for scheduling of the piped futures
  import scala.concurrent.ExecutionContext.Implicits.global

  def receive = {
    case "TEST" => log.info("Received TEST message")
      upStream = sender()
      sender() ! "ACK" // ask for first element

    case "END" => log.info("Received END message, terminating actor")
      queue.complete()

    case elem : Int     => // onPush() + grab(in)
      // "Business" logic
      max_ending_here = Math.max(0, max_ending_here + elem)
      max_so_far = Math.max(max_so_far, max_ending_here)

//      Thread.sleep(100) // FIXME: don't do this at home

      log.info(s"Received: $elem, sending out: $max_so_far")

      val offF = queue.offer(max_so_far)  // push element downstream
      offF pipeTo self                    // generate backpressure in the Actor

    case e : QueueOfferResult =>
      upStream ! "ACK" // ask for next element

    case _ => log.info("Unrecognised message, ignoring")
  }
}

object KadaneFlowActorMain extends App {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  import scala.concurrent.ExecutionContext.Implicits.global

  val actorQueue = Source.queue[Int](10, OverflowStrategy.backpressure)
  val actorSink = actorQueue
    .throttle(1, Duration(100, "millisecond"), 1, ThrottleMode.shaping)
    .to(Sink.foreach((x) => println(x))).run()

  val done = actorSink.watchCompletion()

  val kadaneFlowActor = system.actorOf(Props(new KadaneFlowActor(actorSink)))

  Source.repeat(1).take(100).map(_ => Random.nextInt(1100) - 1000)
    .runWith(Sink.actorRefWithAck(kadaneFlowActor, "TEST", "ACK", "END", _ => "FAIL"))

  done.onComplete(_ => system.terminate())
  Await.ready(system.whenTerminated, Duration.Inf)
}