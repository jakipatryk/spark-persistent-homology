package io.github.jakipatryk.sparkpersistenthomology

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{ BeforeAndAfterAll, Suite }

trait SharedSparkContext extends BeforeAndAfterAll { self: Suite =>

  implicit lazy val spark: SparkSession = {
    val s = SparkSession
      .builder()
      .appName(self.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    s
  }

  def sparkContext: SparkContext = spark.sparkContext

  override def afterAll(): Unit = {
    try {
      spark.stop()
    } finally {
      super.afterAll()
    }
  }

}
