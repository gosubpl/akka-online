package pl.gosub.akka.online

import akka.stream.{Attributes, Inlet, SinkShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}

class StdoutSink extends GraphStage[SinkShape[Char]] {
  val in: Inlet[Char] = Inlet("StdoutSink")
  override val shape: SinkShape[Char] = SinkShape(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      // This requests one element at the Sink startup.
      override def preStart(): Unit = pull(in)

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          print(grab(in))
          pull(in)
        }
      })
    }
}