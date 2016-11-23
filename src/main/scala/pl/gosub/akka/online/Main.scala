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
import com.abahgat.suffixtree.GeneralizedSuffixTree
import com.google.common.hash.{BloomFilter, Funnels}
import org.apache.spark.streamdm.classifiers.trees.HoeffdingTree
import org.apache.spark.streamdm.core.specification._
import org.apache.spark.streamdm.core.{Example, ExampleParser, Instance, TextInstance}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Random


object Main {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  import scala.concurrent.ExecutionContext.Implicits.global

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

    println("now suffix tree")

    val suffixTree = new GeneralizedSuffixTree()

    suffixTree.put("cacao", 0)

    println("Searching: " + suffixTree.search("cac"))

    println("Searching: " + suffixTree.search("caco"))

    Await.ready(system.whenTerminated, Duration.Inf)
  }
}
