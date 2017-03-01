package pl.gosub.akka.online.recursive.least.squares

import akka.Done
import akka.stream._
import akka.stream.stage._

import scala.concurrent.{Future, Promise}

class RecursiveLeastSquaresStage(val rls: RecursiveLeastSquresFilter)
  extends GraphStageWithMaterializedValue[FanInShape2[Double, Double, Double], Future[Done]] {

    // Stage syntax
    val dataIn: Inlet[Double] = Inlet("RecursiveLeastSquaresStage.dataIn")
    val resultsIn: Inlet[Double] = Inlet("RecursiveLeastSquaresStage.resultsIn")
    val predictionsOut: Outlet[Double] = Outlet("RecursiveLeastSquaresStage.predictionsOut")

    override val shape: FanInShape2[Double, Double, Double] = new FanInShape2(dataIn, resultsIn, predictionsOut)

    // Stage semantics
    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
      // Completion notification
      val p: Promise[Done] = Promise()

      val logic = new GraphStageLogic(shape) {

        setHandler(resultsIn, new InHandler {
          @scala.throws[Exception](classOf[Exception])
          override def onPush(): Unit = {
            val nextResult = grab(resultsIn)
            read(dataIn)({ x =>
              if (isAvailable(predictionsOut)) push(predictionsOut, rls.predict(x, nextResult))
            }, () => {})
          }
        })

        setHandler(dataIn, new InHandler {
          override def onPush(): Unit = {
            val x = grab(dataIn)
            read(resultsIn)({previousResult =>
              if (isAvailable(predictionsOut)) push(predictionsOut, rls.predict(x, previousResult))
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
