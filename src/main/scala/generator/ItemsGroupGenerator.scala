package generator

object ItemsGroupGenerator {
  val itemsCount = 200
  val normalSizeDeviation = 1000

  def main(args: Array[String]) {
    import DataGenerator._
    val separatedData = (1 to itemsCount).map(itemId =>
      (itemId, generate(normalSizeDeviation))
    )
    val currentTimestamp = (System.currentTimeMillis / 1000).toInt
    val mixedData = mix(currentTimestamp, separatedData)
    mixedData.foreach {
      case (id, time, DeviceSnapshot(speed, temperature, _)) =>
        println(f"$time,$id,$speed%.2f,$temperature%.2f")
    }
  }


  def mix(time:Int, separatedData: Seq[(Int, Iterable[DeviceSnapshot])]):Seq[(Int, Int, DeviceSnapshot)] = {
    val nonEmpty = separatedData.filter(_._2.nonEmpty)

    if (nonEmpty.isEmpty) {
      List()
    }
    else {
      val itemsToAdd = nonEmpty.map {
        case (id, history) => (id, time, history.head)
      }
      val restOfEvents = nonEmpty.map {
        case (id, history) => (id, history.tail)
      }
      itemsToAdd ++ mix(time + 1, restOfEvents)
    }
  }

}
