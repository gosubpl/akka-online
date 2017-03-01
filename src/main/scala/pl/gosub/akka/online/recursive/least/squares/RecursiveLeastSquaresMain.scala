package pl.gosub.akka.online.recursive.least.squares

import akka.actor.ActorSystem
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape}

import scala.util.Random

object ResursiveLeastSquaresMain extends App {

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  val rlsStage = new RecursiveLeastSquaresStage(new RecursiveLeastSquresFilter(1.0, 10.0))

  val graph = RunnableGraph.fromGraph(GraphDSL.create(){ implicit builder =>

    import GraphDSL.Implicits._

    val cross = builder.add(rlsStage)

    val x = Source.fromIterator(() => Iterator.iterate(0.0)(x => x + 1 ))
    val y = Source.fromIterator(() => Iterator.iterate(-10.0)(x => x + 10 + random(5.0) ))
    val p = Sink.foreach(println)

    x ~> cross.in0
    y ~> cross.in1
    p <~ cross.out

    ClosedShape
  }).run

  def random(sigma: Double): Double = (Random.nextDouble() * sigma) - (sigma / 2.0)
}