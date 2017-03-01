package pl.gosub.akka.online.follow.the.leader

import akka.actor.ActorSystem
import akka.stream.scaladsl.{GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape}

import scala.util.Random

object FollowTheLeaderMain extends App {

  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()

  val hypotheses = (0 to 30).toSeq.map(a => {(x: Double) => a * x})

  val ftlStage = new FollowTheLeaderStage(new FollowTheLeaderLogic(
    (0 to 30).map(a => {(x: Double) => a * x}),
    {(prediction: Double, y: Double) => Math.abs(prediction - y)},
    5
  ))

  val graph = RunnableGraph.fromGraph(GraphDSL.create(){ implicit builder =>

    import GraphDSL.Implicits._

    val cross = builder.add(ftlStage)

    val x = Source.fromIterator(() => Iterator.iterate(0.0)(x => x + 1 ))
    val y = Source.fromIterator(() => Iterator.iterate(0.0)(x => x + 10 + random(5.0) ))
    val p = Sink.foreach(println)

    x ~> cross.in0
    y ~> cross.in1
    p <~ cross.out

    ClosedShape
  }).run

  def random(sigma: Double): Double = (Random.nextDouble() * sigma) - (sigma / 2.0)
}