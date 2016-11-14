package pl.gosub.akka.online

import akka.actor.{Actor, ActorSystem, PoisonPill, Props}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse, Uri}
import akka.stream.{ActorMaterializer, ThrottleMode}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

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

object Main {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  import scala.concurrent.ExecutionContext.Implicits.global

  val myActor = system.actorOf(Props[MyActor])

  def main(args: Array[String]): Unit = {
    println("Hello from main")

    val reqResponseFlow = Flow[HttpRequest].map[HttpResponse] (_ match {
      case HttpRequest(HttpMethods.GET, Uri.Path("/"), _, _, _) =>
        HttpResponse(200, entity = "Hello!")

      case _ => HttpResponse(200, entity = "Ooops, not found")
    })

    Http().bindAndHandle(reqResponseFlow, "localhost", 8888)

    system.scheduler.schedule(Duration(100, "millisecond"), Duration(50, "millisecond"), myActor, "KABOOM !!!")

    val stdoutSink = new StdoutSink

    val done =
      Source
      .repeat("Hello")
      .zip(Source.fromIterator(() => Iterator.from(0)))
      .take(7)
      .mapConcat{
        case (s, n) =>
          val i = " " * n
          f"$i$s%n"
      }
      .throttle(42, Duration(1500, "millisecond"), 1, ThrottleMode.Shaping)
      .runWith(Sink.actorRefWithAck(myActor, "test", "ack", "end", _ => "fail"))

    //done.onComplete(_ => system.terminate())



    Await.ready(system.whenTerminated, Duration.Inf)
  }
}
