package generator

object OneItemGenerator {
  val normalSizeDeviation = 100

  def main(args: Array[String]) {
    import DataGenerator._

    val lines = generate(normalSizeDeviation).map {
      case DeviceSnapshot(speed, temperature, fail) =>
        val failVal = if (fail) 1 else 0
        f"$speed%.2f,$temperature%.2f,$failVal"
    }
    println(lines.mkString("\n"))
  }
}
