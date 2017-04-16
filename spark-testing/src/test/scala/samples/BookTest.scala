package samples

import com.holdenkarau.spark.testing.RDDComparisons
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner



@RunWith(classOf[JUnitRunner])
class BookTest extends FunSuite with SharedSparkContext with RDDComparisons {
  
  System.setProperty("hadoop.home.dir", "C:\\hadoop")
  
    test("test RDDComparisons") {
    val expectedRDD = sc.parallelize(Seq(1, 2, 3))
    val resultRDD = sc.parallelize(Seq(3, 2, 1))

    assert(None === compareRDD(expectedRDD, resultRDD)) // succeed
    assert(None === compareRDDWithOrder(expectedRDD, resultRDD)) // Fail

    assertRDDEquals(expectedRDD, resultRDD) // succeed
    assertRDDEqualsWithOrder(expectedRDD, resultRDD) // Fail//
  }
  
}