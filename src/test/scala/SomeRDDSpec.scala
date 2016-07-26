import com.holdenkarau.spark.testing.{RDDGenerator, SharedSparkContext}
import org.apache.spark.rdd.RDD
import org.scalacheck.Gen
import org.scalatest.FlatSpec
import org.scalatest.prop.PropertyChecks

class SomeRDDSpec extends FlatSpec with SharedSparkContext with PropertyChecks {
  "spark" should "execute some commands" in {
    forAll(RDDGenerator.genRDD[String](sc)(Gen.alphaStr)){
      rdd => rdd.map(_.length).count() == rdd.count()
    }
  }
}
