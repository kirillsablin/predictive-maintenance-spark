package generator

import scala.util.Random

object DataGenerator {
  val deviationFactor = 0.03
  val baseSpeed = 50.0
  val baseTemperature = 80.0
  val badSizeDeviation = 10
  val minBadSize = 10
  val badStatusDetectionLag = 3
  val speedChangeFactor = 0.95
  val temperatureChangeFactor = 1.06
  val minNormalSize = 7

  def generate(normalSizeDeviation: Int): Iterable[DeviceSnapshot] = {
    val normalSize = minNormalSize + Random.nextInt(normalSizeDeviation)
    val badSize = minBadSize + Random.nextInt(badSizeDeviation)

    (1 to normalSize).map(_ => DeviceSnapshot(deviateDouble(baseSpeed), deviateDouble(baseTemperature), maintenanceNeeded = false)) ++
      (1 to badSize).foldLeft(((baseSpeed, baseTemperature), List[DeviceSnapshot]())) {
        case (((currentSpeed, currentTemperature), soFar), num) =>
          val newSpeed = currentSpeed * speedChangeFactor
          val newTemperature = currentTemperature * temperatureChangeFactor
          ((newSpeed, newTemperature), DeviceSnapshot(newSpeed, newTemperature, num >= badStatusDetectionLag) :: soFar)
      }._2.reverse
  }

  private[this] def deviateDouble(base: Double): Double = base + base * deviationFactor * (0.5 - Math.random())
}
