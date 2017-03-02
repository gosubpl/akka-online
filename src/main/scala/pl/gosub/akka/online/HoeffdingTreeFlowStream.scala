package pl.gosub.akka.online

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.stream._
import akka.stream.scaladsl.{GraphDSL, Merge, RunnableGraph, Sink, Source}
import com.sun.xml.internal.ws.util.StreamUtils
import org.apache.spark.streamdm.classifiers.trees.HoeffdingTree
import org.apache.spark.streamdm.core.{Example, ExampleParser}
import org.apache.spark.streamdm.core.specification.{ExampleSpecification, SpecificationParser}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class HoeffdingTreeProcessor(val schema: ExampleSpecification) {
  // state
  val hTree = new HoeffdingTree
  hTree.init(schema)
  var count = 0

  def process(query: LearnerQuery) = {
    query.queryType match {
      case "EXAMPLE" =>
        hTree.trainIncremental(query.elem)
        count += 1
        "Received: " + count + " " + query.elem.toString + ", learned\n"

      case "QUERY" =>
        val answer = hTree.predictSingle(query.elem)
        count += 1
        "Received: " + count + " " + query.elem.toString + ", generated prediction " + answer._2 + "\n"

      case _ => "Unrecognised message, ignoring"
    }
  }
}

object HoeffdingTreeFlowStreamMain extends App {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  val specParser = new SpecificationParser

  val arffPath = this.getClass.getResource("/elecNormNew.arff").getPath

  val exampleSpec = specParser.fromArff(arffPath)

  val example1 = ExampleParser.fromArff("0,2,0.085106,0.042482,0.251116,0.003467,0.422915,0.414912,DOWN", exampleSpec)
  val example2 = ExampleParser.fromArff("0,2,0.255319,0.051489,0.298721,0.003467,0.422915,0.414912,UP", exampleSpec)
  val example3 = ExampleParser.fromArff("0.424627,6,0.234043,0.070854,0.108004,0.003467,0.422915,0.414912,DOWN", exampleSpec)
  val example4 = ExampleParser.fromArff("0.424627,6,0.170213,0.070854,0.070515,0.003467,0.422915,0.414912,UP", exampleSpec)

  /*
    val learn1 = LearnerQuery("EXAMPLE", example1)
    val learn2 = LearnerQuery("EXAMPLE", example2)
    val query1 = LearnerQuery("QUERY", example1)
    val query2 = LearnerQuery("QUERY", example2)
    val query3 = LearnerQuery("QUERY", example3)
    val query4 = LearnerQuery("QUERY", example4)
  */

  val examples = List(example1, example2)
  val queries = List(example1, example2, example3, example4, example2)

  val masterControlProgram = RunnableGraph.fromGraph(GraphDSL.create(Sink.foreach(print)) { implicit builder =>
    outMatches =>
      import GraphDSL.Implicits._
      val inExamples = Source.fromIterator(() => examples.toIterator).throttle(1, Duration(2500, "millisecond"), 1, ThrottleMode.shaping)
      val inQueries = Source.fromIterator(() => queries.toIterator).throttle(1, Duration(1000, "millisecond"), 1, ThrottleMode.shaping)

      val taggedExamples = inExamples.map(LearnerQuery("EXAMPLE", _))
      val taggedQueries = inQueries.map(LearnerQuery("QUERY", _))
      val fanIn = builder.add(Merge[LearnerQuery](2))

      taggedExamples ~> fanIn.in(0);
      fanIn.out.statefulMapConcat(() => {
        val proc = new HoeffdingTreeProcessor(exampleSpec)
        proc.process(_)
      }) ~> outMatches
      taggedQueries ~> fanIn.in(1)
      ClosedShape
  }).run()

  import scala.concurrent.ExecutionContext.Implicits.global

  masterControlProgram.onComplete(_ => system.terminate())
  Await.ready(system.whenTerminated, Duration.Inf)

}
