import java.nio.file.FileSystems

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, ThrottleMode}
import akka.stream.scaladsl.{GraphDSL, Merge, RunnableGraph, Sink, Source}
import akka.stream.alpakka.file.scaladsl
import org.apache.spark.streamdm.core.ExampleParser
import org.apache.spark.streamdm.core.specification.SpecificationParser
import pl.gosub.akka.online.{HoeffdingTreeProcessor, LearnerQuery}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.duration._


object HoeffdingTreeWithAlpakka extends App {
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  val specParser = new SpecificationParser

  val arffPath = this.getClass.getResource("/elecNormNew.arff").getPath // add for Windows .replaceFirst("^/(.:/)", "$1")

  val exampleSpec = specParser.fromArff(arffPath)

  val fsPath = this.getClass.getResource("/elecNormData.txt").getPath // add for Windows .replaceFirst("^/(.:/)", "$1")
  println(fsPath)


  val fs = FileSystems.getDefault
  val lines: Source[String, NotUsed] = scaladsl.FileTailSource.lines(
    path = fs.getPath(fsPath),
    maxLineSize = 8192,
    pollingInterval = 250.millis
  )

//  if the lines below do not work, please make sure that you got the linefeed character right wrt your operating system (LF vs CRLF)
//  lines.map(line => LearnerQuery(line.split(";").apply(0), ExampleParser.fromArff(line.split(";").apply(1), exampleSpec)))
//    .runForeach(line => System.out.println(line))

  val masterControlProgram = RunnableGraph.fromGraph(GraphDSL.create(Sink.foreach(print)) { implicit builder =>
    outMatches =>
      import GraphDSL.Implicits._
      val taggedInput = lines.map(line => LearnerQuery(line.split(";").apply(0), ExampleParser.fromArff(line.split(";").apply(1), exampleSpec)))

      taggedInput.statefulMapConcat(() => {
        val proc = new HoeffdingTreeProcessor(exampleSpec)
        proc.process(_)
      }) ~> outMatches
      ClosedShape
  }).run()

  import scala.concurrent.ExecutionContext.Implicits.global

  masterControlProgram.onComplete(_ => system.terminate())
  Await.ready(system.whenTerminated, Duration.Inf)

}
