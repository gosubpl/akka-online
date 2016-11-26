package pl.gosub.akka.online

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.event.Logging
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult, ThrottleMode}
import akka.stream.scaladsl.{Sink, Source, SourceQueueWithComplete}
import org.apache.spark.streamdm.classifiers.trees.HoeffdingTree
import org.apache.spark.streamdm.core.{Example, ExampleParser}
import org.apache.spark.streamdm.core.specification.{ExampleSpecification, SpecificationParser}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import akka.pattern.pipe // For the pipeTo pattern

case class LearnerQuery(queryType : String, elem : Example)

class HoeffdingTreeFlowActor (queue: SourceQueueWithComplete[(Example, Double)], schema: ExampleSpecification) extends Actor {
  val log = Logging(context.system, this)
  // state
  val hTree = new HoeffdingTree
  hTree.init(schema)
  var upStream : ActorRef = self

  // Execution context for scheduling of the piped futures
  import scala.concurrent.ExecutionContext.Implicits.global

  def receive = {
    case "TEST" => log.info("Received TEST message")
      upStream = sender()
      sender() ! "ACK" // ask for first element

    case "END" => log.info("Received END message, terminating actor")
      queue.complete()

    case query : LearnerQuery     => // onPush() + grab(in)
      // "Business" logic

      query.queryType match {
        case "EXAMPLE" =>
          hTree.trainIncremental(query.elem)
          log.info(s"Received: ${query.elem.toString}, learned")
          val futureF = Future.successful(QueueOfferResult.Enqueued)
          futureF pipeTo self

        case "QUERY" =>
          val answer = hTree.predictSingle(query.elem)
          log.info(s"Received: ${query.elem.toString}, generating prediction")
          val offF = queue.offer(answer)  // push element downstream
          offF pipeTo self                    // generate backpressure in the Actor

        case _ => // ignore but continue consuming
          val futureF = Future.successful(QueueOfferResult.Enqueued)
          futureF pipeTo self

      }

    case e : QueueOfferResult =>
      upStream ! "ACK" // ask for next element

    case _ => log.info("Unrecognised message, ignoring")
  }
}

object HoeffdingTreeFlowActorMain extends App {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  import scala.concurrent.ExecutionContext.Implicits.global

  val actorQueue = Source.queue[(Example, Double)](10, OverflowStrategy.backpressure)
  val actorSink = actorQueue
    .throttle(1, Duration(100, "millisecond"), 1, ThrottleMode.shaping)
    .to(Sink.foreach((x) => println(x.toString()))).run()

  val done = actorSink.watchCompletion()

  val specParser = new SpecificationParser

  val exampleSpec = specParser.fromArff("/home/janek/Downloads/arff/elecNormNew.arff")

  val hoeffdingTreeFlowActor = system.actorOf(Props(new HoeffdingTreeFlowActor(actorSink, exampleSpec)))

  val example1 = ExampleParser.fromArff("0,2,0.085106,0.042482,0.251116,0.003467,0.422915,0.414912,DOWN", exampleSpec)
  val example2 = ExampleParser.fromArff("0,2,0.255319,0.051489,0.298721,0.003467,0.422915,0.414912,UP", exampleSpec)
  val example3 = ExampleParser.fromArff("0.424627,6,0.234043,0.070854,0.108004,0.003467,0.422915,0.414912,DOWN", exampleSpec)
  val example4 = ExampleParser.fromArff("0.424627,6,0.170213,0.070854,0.070515,0.003467,0.422915,0.414912,UP", exampleSpec)


  val learn1 = LearnerQuery("EXAMPLE", example1)
  val learn2 = LearnerQuery("EXAMPLE", example2)
  val query1 = LearnerQuery("QUERY", example1)
  val query2 = LearnerQuery("QUERY", example2)
  val query3 = LearnerQuery("QUERY", example3)
  val query4 = LearnerQuery("QUERY", example4)

  val examples = List(learn1, query1, query2, query3, query4, learn2, query2)

  Source.fromIterator(() => examples.toIterator)
    .throttle(1, Duration(1000, "millisecond"), 1, ThrottleMode.shaping)
    .runWith(Sink.actorRefWithAck(hoeffdingTreeFlowActor, "TEST", "ACK", "END", _ => "FAIL"))

  done.onComplete(_ => system.terminate())
  Await.ready(system.whenTerminated, Duration.Inf)
}