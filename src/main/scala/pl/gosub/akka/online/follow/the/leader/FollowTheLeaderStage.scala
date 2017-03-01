package pl.gosub.akka.online.follow.the.leader

import akka.Done
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler, OutHandler}
import akka.stream.{Attributes, FanInShape2, Inlet, Outlet}

import scala.concurrent.{Future, Promise}

class FollowTheLeaderStage(private val ftl: FollowTheLeaderLogic) extends GraphStageWithMaterializedValue[FanInShape2[Double, Double, Double], Future[Done]]{

  // Stage syntax
  val dataIn: Inlet[Double] = Inlet("FollowTheLeaderStage.dataIn")
  val resultsIn: Inlet[Double] = Inlet("FollowTheLeaderStage.resultsIn")
  val predictionsOut: Outlet[Double] = Outlet("FollowTheLeaderStage.predictionsOut")

  override val shape: FanInShape2[Double, Double, Double] = new FanInShape2(dataIn, resultsIn, predictionsOut)

  @scala.throws[Exception](classOf[Exception])
  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    // Completion notification
    val p: Promise[Done] = Promise()

    val logic = new GraphStageLogic(shape) {

      setHandler(resultsIn, new InHandler {
        @scala.throws[Exception](classOf[Exception])
        override def onPush(): Unit = {
          val nextResult = grab(resultsIn)
          read(dataIn)({ x =>
            if (isAvailable(predictionsOut)) push(predictionsOut, ftl.predict(x, nextResult))
          }, () => {})
        }
      })


      setHandler(dataIn, new InHandler {
        override def onPush(): Unit = {
          val x = grab(dataIn)
          read(resultsIn)({previousResult =>
            if (isAvailable(predictionsOut)) push(predictionsOut, ftl.predict(x, previousResult))
          }, () => {})
        }

        override def onUpstreamFinish(): Unit = {
          completeStage()
        }
      })

      setHandler(predictionsOut, new OutHandler {
        override def onPull(): Unit = {
          pull(dataIn)
        }
      })
    }

    (logic, p.future)
  }
}