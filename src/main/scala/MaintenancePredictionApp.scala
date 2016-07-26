import com.holdenkarau.spark.testing.TestInputStream
import generator.ItemsGroupGenerator
import org.apache.spark._
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

import scala.io.Source

object MaintenancePredictionApp {
  val LRThreshold = 0.8

  val testDataPath = "data/streams/stream.1"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Maintenance Prediction Streaming")
    val ssc = new StreamingContext(conf, Seconds(1))
    val model = LogisticRegressionModel.load(ssc.sparkContext, LearnerApp.modelPath)

    val data = createDStream(ssc)

    val featuresPerDevice = Transformers.windowToFeatures(data)

    val maintenanceSignals = featuresPerDevice.filter(f => model.predict(f._2) > LRThreshold)

    processMaintenanceSignals(maintenanceSignals)

    ssc.start()
    ssc.awaitTermination()
  }

  def createDStream(ssc: StreamingContext): DStream[(Int, (Double, Double))] = {
    val whole = Source.fromFile(testDataPath).getLines().map(line => {
      val elems = line.split(",")
      (elems(1).toInt, (elems(2).toDouble, elems(3).toDouble))
    })
    new TestInputStream(ssc.sparkContext, ssc, whole.grouped(ItemsGroupGenerator.itemsCount).toSeq, 2)
  }

  def processMaintenanceSignals(maintenanceSignals: DStream[(Int, Vector)]): Unit = {
    maintenanceSignals.map(f => f._1 + " device requires maintenance").print()
  }
}
