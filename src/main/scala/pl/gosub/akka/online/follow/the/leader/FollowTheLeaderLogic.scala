package pl.gosub.akka.online.follow.the.leader

class FollowTheLeaderLogic(val hypotheses: Seq[Double => Double], lossFunction: ((Double, Double) => Double), windowSize: Int) {

  private var past: Seq[(Double, Double)] = Seq.empty;

  private var pastX = 0.0

  def predict(x: Double, y: Double): Double = {

    past = past :+ (pastX, y)

    past.dropWhile(_ => past.size >= windowSize)

    val leader = if(past isEmpty) {
      hypotheses.head
    } else {
      hypotheses
        .map(hypothesis => (hypothesis, past.map{ case (x, y) => lossFunction(hypothesis(x), y)}.reduce(_+_)))
        .sortBy(_._2)
        .map(_._1)
        .head
    }

    val prediction = leader(x)

    pastX = x

    prediction
  }
}
