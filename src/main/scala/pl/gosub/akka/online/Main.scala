package pl.gosub.akka.online

import akka.actor.{Actor, ActorSystem, PoisonPill, Props}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse, Uri}
import akka.stream.{ActorMaterializer, ThrottleMode}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Random

class MyActor extends Actor {
  val log = Logging(context.system, this)

  def receive = {
    case "test" => log.info("received test")
      sender() ! "ack"
    case "end" => log.info("terminating actor")
      self ! PoisonPill
    case s : String     => log.info(s)
     case s : Char     => log.info(s"$s")
      sender() ! "ack"
    case _ => log.info("really bad")
   }
}

class KadaneActor extends Actor {
  val log = Logging(context.system, this)
  var max_ending_here = 0
  var max_so_far = 0

  def receive = {
    case "test" => log.info("received test")
      sender() ! "ack"
    case "end" => log.info("terminating actor")
      self ! PoisonPill
    case s : String     => log.info(s)
    case x : Int     =>
      max_ending_here = Math.max(0, max_ending_here + x)
      max_so_far = Math.max(max_so_far, max_ending_here)
      Thread.sleep(100)
      log.info(s"received: $x, max ending here: $max_ending_here, max so far: $max_so_far")
      sender() ! "ack"
    case _ => log.info("really bad")
  }
}

object Main {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  import scala.concurrent.ExecutionContext.Implicits.global

  val myActor = system.actorOf(Props[MyActor])
  val kadaneActor = system.actorOf(Props[KadaneActor])

  def main(args: Array[String]): Unit = {
    println("Hello from main")

    val reqResponseFlow = Flow[HttpRequest].map[HttpResponse] (_ match {
      case HttpRequest(HttpMethods.GET, Uri.Path("/"), _, _, _) =>
        HttpResponse(200, entity = "Hello!")

      case _ => HttpResponse(200, entity = "Ooops, not found")
    })

    Http().bindAndHandle(reqResponseFlow, "localhost", 8888)

//    system.scheduler.schedule(Duration(100, "millisecond"), Duration(50, "millisecond"), myActor, "KABOOM !!!")
//
//    val stdoutSink = new StdoutSink
//
//    val done =
//      Source
//      .repeat("Hello")
//      .zip(Source.fromIterator(() => Iterator.from(0)))
//      .take(7)
//      .mapConcat{
//        case (s, n) =>
//          val i = " " * n
//          f"$i$s%n"
//      }
//      .throttle(42, Duration(1500, "millisecond"), 1, ThrottleMode.Shaping)
//      .runWith(Sink.actorRefWithAck(myActor, "test", "ack", "end", _ => "fail"))

    //done.onComplete(_ => system.terminate())
    /*
    def max_subarray(A):
    max_ending_here = max_so_far = 0
    for x in A:
        max_ending_here = max(0, max_ending_here + x)
        max_so_far = max(max_so_far, max_ending_here)
    return max_so_far
     */

    Source.repeat(1).take(100).map(_ => Random.nextInt(1100) - 1000).runWith(Sink.actorRefWithAck(kadaneActor, "test", "ack", "end", _ => "fail"))

//    Source.repeat(1).take(100).map(_ => Random.nextInt(1100) - 1000).runWith(Sink.actorRef(kadaneActor, "test")) // bad, won't work as no backpressure

    Await.ready(system.whenTerminated, Duration.Inf)
  }
}
