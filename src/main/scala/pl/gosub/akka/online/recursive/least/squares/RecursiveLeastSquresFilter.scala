package pl.gosub.akka.online.recursive.least.squares

class RecursiveLeastSquresFilter(private val x0: Double, private val y0: Double) {

  private var theta: Double = 1.0;

  private var previousResult: Double = y0

  private var weight = if(x0 == 0) 0.0 else (y0 / x0)

  def predict(x: Double, previousResult: Double): Double = {

    theta = theta - (theta * theta * x * x) / (1 + x * x * theta)

    weight = weight - theta * x * (weight * x - previousResult)

    val prediction = weight * x

    prediction
  }
}