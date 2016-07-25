import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class ModelSpec extends FlatSpec with SharedSparkContext with Matchers with BeforeAndAfter {

  var model: GeneralizedLinearModel = _

  before {
    model = LogisticRegressionModel.load(sc, LearnerApp.modelPath)
  }

  val badDeviceData = Vectors.dense(50.46, 47.50, 45.0, 42.7, 40.3, 78.99, 84.20, 90.1, 95.02, 100.8)
  val goodDeviceData = Vectors.dense(50.03, 49.3, 50.13, 50.3, 50.01, 79.9, 80.5, 79.6, 79.8, 80.1)

  "model" should "detect failure" in {
    model.predict(badDeviceData) should be > MaintenancePredictionApp.LRThreshold
  }

  it should "not recognize device in good state as bad" in {
    model.predict(goodDeviceData) should be < (1 - MaintenancePredictionApp.LRThreshold)
  }

}
