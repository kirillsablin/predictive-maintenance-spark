import com.holdenkarau.spark.testing.{SharedSparkContext, TestInputStream}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.streaming.{ClockAccessor, Seconds, StreamingContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

class TransformersSpec extends FlatSpec with Matchers with SharedSparkContext with Eventually {

  override val conf = new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", appID).
    set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")

  val data = Seq(
    Seq(
      (1, (1d, 2d)),
      (2, (10d, 20d))
    ),
    Seq(
      (1, (3d, 4d))
    ),
    Seq(
      (1, (5d, 6d))
    ),
    Seq(
      (1, (7d, 8d))
    ),
    Seq(
      (1, (9d, 10d))
    )
  )

  val expected = (1, Vectors.dense(1d, 3d, 5d, 7d, 9d, 2d, 4d, 6d, 8d, 10d))

  "windowToFeatures" should "transform sliding window of RDD to stream of feature vectors" in {
    val ssc = new StreamingContext(sc, Seconds(1))
    var results = ListBuffer.empty[(Int, Vector)]
    val inputDS = new TestInputStream(sc, ssc, data, 2)

    val outputDS = Transformers.windowToFeatures(inputDS)

    outputDS.foreachRDD(rdd => { rdd.foreach(row =>  { println(row); results += row  }) })

    ssc.start()
    ClockAccessor.advance(ssc, Seconds(6)) // more than window

    eventually {
      println(results)
      results.last should be(expected)
    }

  }

}
