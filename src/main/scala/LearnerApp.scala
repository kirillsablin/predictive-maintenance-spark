import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.reflect.io.Path

object LearnerApp {
  val windowSize = 5
  val numIterations = 10

  val modelPath = "model.data"
  val trainingSamplesPath = "data/training"
  val testSamplesPath = "data/test"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Learner application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val trainingData = loadFrom(sc, trainingSamplesPath)

    val model = LogisticRegressionWithSGD.train(trainingData, numIterations)

    val testData = loadFrom(sc, testSamplesPath)

    val valuesAndPred = testData.map((point: LabeledPoint) => {
      val prediction = model.predict(point.features)
      (point.label, prediction)
    })
    val mse = valuesAndPred.map { case (v, p) => math.pow(v - p, 2) }.mean()

    println("training Mean Squared Error = " + mse)

    saveModel(sc, model)
  }

  def saveModel(sc: SparkContext, model: LogisticRegressionModel): Unit = {
    Path(modelPath).deleteRecursively()
    model.save(sc, modelPath)
  }

  def loadFrom(sc: SparkContext, loadPath: String): RDD[LabeledPoint] = {
    val unpreparedData = sc.wholeTextFiles(loadPath)

    val preparedData = unpreparedData.flatMap {
      case (_, deviceData) =>
        val lines = deviceData.split("\n")
        lines.map(a => {
          val elems = a.split(",")
          val speed = elems(0).toDouble
          val temperature = elems(1).toDouble
          val needMaintenance = elems(2).toDouble
          (speed, temperature, needMaintenance)
        }).sliding(windowSize).map(a =>
          LabeledPoint(a.last._3, Vectors.dense(a.map(_._1) ++ a.map(_._2)))
        )
    }
    preparedData
  }
}