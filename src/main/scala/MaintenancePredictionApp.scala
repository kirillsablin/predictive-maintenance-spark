import com.holdenkarau.spark.testing.TestInputStream
import generator.ItemsHiveGenerator
import org.apache.spark._
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

import scala.io.Source

object MaintenancePredictionApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Maintenance Prediction Streaming")
    val ssc = new StreamingContext(conf, Seconds(1))
    val model = LogisticRegressionModel.load(ssc.sparkContext, "model.data")

    val data = createTestDStream(ssc)
    val featuresPerDevice = data.window(Seconds(LearnerApp.windowSize + 2), Seconds(1)).groupByKey().
      filter(_._2.size >= LearnerApp.windowSize).
      map({
        case (k, v) =>
          val valuesToConsider = v.takeRight(LearnerApp.windowSize)
          (k, Vectors.dense((valuesToConsider.map(_._1) ++ valuesToConsider.map(_._2)).toArray))
      })

    val maintenanceSignals = featuresPerDevice.filter(f => model.predict(f._2) > 0.8)

    maintenanceSignals.map(f => f._1 + " device requires maintenance").print()

    ssc.start()
    ssc.awaitTermination()
  }

  def createTestDStream(ssc: StreamingContext): DStream[(Int, (Double, Double))] = {
    val whole = Source.fromFile("data/streams/stream.1").getLines().map(s => {
      val elems = s.split(",")
      (elems(1).toInt, (elems(2).toDouble, elems(3).toDouble))
    })
    new TestInputStream(ssc.sparkContext, ssc, whole.grouped(ItemsHiveGenerator.itemsCount).toSeq, 2)
  }
}
