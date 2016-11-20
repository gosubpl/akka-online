package pl.gosub.akka.online

import akka.NotUsed
import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse, Uri}
import akka.stream.QueueOfferResult.Enqueued
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, RunnableGraph, Sink, Source, SourceQueueWithComplete}
import akka.stream._

import scala.concurrent.Future
import akka.pattern.pipe
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.google.common.hash.{BloomFilter, Funnels}
import org.apache.spark.streamdm.classifiers.trees.HoeffdingTree
import org.apache.spark.streamdm.core.specification._
import org.apache.spark.streamdm.core.{Example, ExampleParser, Instance, TextInstance}

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

class KadaneActorFlow(queue: SourceQueueWithComplete[Int]) extends Actor {
  val log = Logging(context.system, this)
  var max_ending_here = 0
  var max_so_far = 0
  var upStream : ActorRef = self

  import scala.concurrent.ExecutionContext.Implicits.global

  def receive = {
    case "test" => log.info("received test")
      upStream = sender()
      sender() ! "ack"
    case "end" => log.info("terminating actor")
      self ! PoisonPill
    case s : String     => log.info(s)
    case x : Int     => // onPush() + grab(in)
      max_ending_here = Math.max(0, max_ending_here + x)
      max_so_far = Math.max(max_so_far, max_ending_here)
      Thread.sleep(100)
      log.info(s"received: $x, max ending here: $max_ending_here, max so far: $max_so_far")
      val offF = queue.offer(max_so_far) //push/emit(element downstream)
      offF pipeTo self
    case e : QueueOfferResult =>
      upStream ! "ack" // pull(in)
    case _ => log.info("really bad")
  }
}

class KadaneStage extends GraphStage[FlowShape[Int, Int]] {

  val in: Inlet[Int] = Inlet("KadaneStage.in")
  val out: Outlet[Int] = Outlet("KadaneStage.out")
  override val shape: FlowShape[Int, Int] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      var maxEndingHere = 0
      var maxSoFar = 0

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val x = grab(in)
          maxEndingHere = Math.max(0, maxEndingHere + x)
          maxSoFar = Math.max(maxSoFar, maxEndingHere)
          println(s"stage: $x, maxEndingHere: $maxEndingHere, maxSoFar: $maxSoFar")
//          if (isAvailable(out))
//            push(out, maxSoFar)
          emit(out, maxSoFar)
//          pull(in)
        }

        override def onUpstreamFinish(): Unit = {
//          emit(out, maxSoFar)
          completeStage()
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          if (!hasBeenPulled(in))
            pull(in)
        }
      })

    }

}

class BloomFilterCrossStage extends GraphStage[BidiShape[Int, Int, Int, String]] {

  val dataIn: Inlet[Int] = Inlet("BloomFilterCrossStage.dataIn")
  val dataOut: Outlet[Int] = Outlet("BloomFilterCrossStage.dataOut")
  val queriesIn: Inlet[Int] = Inlet("BloomFilterCrossStage.queriesIn")
  val answersOut: Outlet[String] = Outlet("BloomFilterCrossStage.answersOut")
  override val shape: BidiShape[Int, Int, Int, String] = BidiShape(dataIn, dataOut, queriesIn, answersOut)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      var maxEndingHere = 0
      var maxSoFar = 0
      val filter = BloomFilter.create[Integer](Funnels.integerFunnel(), 1000, 0.01)

      setHandler(dataIn, new InHandler {
        override def onPush(): Unit = {
          val x = grab(dataIn)
          filter.put(x)
          println(s"object $x put into filter")
          if (isAvailable(dataOut))
            push(dataOut, x)
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
            s"YES, filter probably contains $x"
          } else {
            s"NO, filter probably does not contain $x"
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


object Main {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  import scala.concurrent.ExecutionContext.Implicits.global

  val sourceQueue = Source.queue[Int](10, OverflowStrategy.backpressure)
  val qSink = sourceQueue.to(Sink.foreach((x) => println(x))).run()

  val myActor = system.actorOf(Props[MyActor])
  val kadaneActor = system.actorOf(Props[KadaneActor])
  val kadaneActorFlow = system.actorOf(Props(new KadaneActorFlow(qSink)))

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

//    Source.repeat(1).take(100).map(_ => Random.nextInt(1100) - 1000).runWith(Sink.actorRefWithAck(kadaneActorFlow, "test", "ack", "end", _ => "fail"))

//    Source.repeat(1).take(100).map(_ => Random.nextInt(1100) - 1000).runWith(Sink.actorRef(kadaneActor, "test")) // bad, won't work as no backpressure

    val kadaneStage = new KadaneStage
    val crossStage = new BloomFilterCrossStage

//    Source.repeat(1).take(100).map(_ => Random.nextInt(1100) - 1000).via(kadaneStage).runWith(Sink.foreach(println))

//    val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
//      import GraphDSL.Implicits._
//      val inData = Source.repeat(1).take(1000).map(_ => Random.nextInt(1000)).throttle(1, Duration(100, "millisecond"), 1, ThrottleMode.shaping)
//      val outData = Sink.foreach(println)
//      val inControl = Source.repeat(1).take(100).map(_ => Random.nextInt(2000) - 1000).throttle(1, Duration(1500, "millisecond"), 1, ThrottleMode.shaping)
//      val outControl = Sink.foreach(println)
//
//      val cross = builder.add(crossStage)
//
//      inData ~> cross.in1; cross.out1 ~> outData
//      inControl ~> cross.in2; cross.out2 ~> outControl
//      ClosedShape
//    }).run()

    println("Now trying the Hoeffding Tree")

    println("arffStuff")

    val specParser = new SpecificationParser

    val exampleSpec = specParser.fromArff("/home/janek/Downloads/arff/elecNormNew.arff")

    val example1 = ExampleParser.fromArff("0,2,0.085106,0.042482,0.251116,0.003467,0.422915,0.414912,DOWN", exampleSpec)

    val example2 = ExampleParser.fromArff("0,2,0.255319,0.051489,0.298721,0.003467,0.422915,0.414912,UP", exampleSpec)

    println("example Arff " + example1.in.toString + " / " + example1.out.toString)
    println("example Arff2 " + example2.in.toString + " / " + example2.out.toString)

    println("Spec " + exampleSpec.in.size() + " " + exampleSpec.out.size() + " " + exampleSpec.out.isNominal(0))

    println("after arff")

    val hTree = new HoeffdingTree

    hTree.init(exampleSpec)

    hTree.trainIncremental(example1)

    println(hTree.predictSingle(example1)._2)

    hTree.trainIncremental(example2)

    println(hTree.predictSingle(example2)._2)

    Await.ready(system.whenTerminated, Duration.Inf)
  }
}
