import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.streaming.Seconds

object Transformers {

  def windowToFeatures(in: DStream[(Int, (Double, Double))]) : DStream[(Int, Vector)] =
    in.
      window(Seconds(LearnerApp.windowSize + 2), Seconds(1)).
      groupByKey().
      filter(_._2.size >= LearnerApp.windowSize).
      map({
        case (k, measurements) =>
          val measurementsToConsider = measurements.takeRight(LearnerApp.windowSize)
          (k, Vectors.dense((measurementsToConsider.map(_._1) ++ measurementsToConsider.map(_._2)).toArray))
      })
}
