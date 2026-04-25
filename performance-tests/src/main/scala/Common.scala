import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OParser

case class Config(
                   caseName: String = "",
                   inputPath: Option[String] = None,
                   outputPath: Option[String] = None,
                   pointsCloudOutputPath: Option[String] = None
                 )

object Config {

  def parser(pName: String): OParser[Unit, Config] = {
    val builder = OParser.builder[Config]
    import builder._

    OParser.sequence(
      programName(pName),
      opt[String]("case")
        .required()
        .action((x, c) => c.copy(caseName = x))
        .text("specific test case name"),
      opt[String]("inputPath")
        .action((x, c) => c.copy(inputPath = Some(x)))
        .text("location of the input dataset file"),
      opt[String]("outputPath")
        .action((x, c) => c.copy(outputPath = Some(x)))
        .text("location where the output persistence pairs should be saved as CSV"),
      opt[String]("pointsCloudOutputPath")
        .action((x, c) => c.copy(pointsCloudOutputPath = Some(x)))
        .text("location where the generated points cloud should be saved as CSV")
    )
  }

}

object Utils {

  def savePointsCloud(pointsCloud: Dataset[Array[Float]], path: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    pointsCloud
      .coalesce(1)
      .map(_.mkString(","))
      .write
      .text(path)
  }

  def savePersistencePairs(persistencePairs: Dataset[io.github.jakipatryk.sparkpersistenthomology.PersistencePair], path: String)(implicit spark: SparkSession): Unit = {
    persistencePairs
      .coalesce(1)
      .write
      .option("header", "true")
      .csv(path)
  }

}

