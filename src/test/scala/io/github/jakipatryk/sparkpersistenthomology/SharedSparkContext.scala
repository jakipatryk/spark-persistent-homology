package io.github.jakipatryk.sparkpersistenthomology

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{ BeforeAndAfterAll, Suite }

trait SharedSparkContext extends BeforeAndAfterAll { self: Suite =>

  implicit var sparkSession: SparkSession = _

  def sparkContext: SparkContext = sparkSession.sparkContext

  override def beforeAll(): Unit = {
    sparkSession = SparkSession
      .builder()
      .appName(self.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    sparkContext.setLogLevel("ERROR")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try {
      if (sparkSession != null) {
        sparkSession.stop()
      }
    } finally {
      super.afterAll()
    }
  }

}
