package cc.xmccc.pgsm

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

import scala.io.Source

object RandomForestModel {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("RandomForestModel")
      .getOrCreate()

    import sparkSession.implicits._

    val model_path = args(0)
    val feature_file = args(1)

    val model = PipelineModel.load(model_path)
    val random_forest_model = model.stages(1).asInstanceOf[RandomForestClassificationModel]
    val features = Source.fromFile(feature_file)
        .getLines()
        .toList
        .map(_.split(",").toList)
        .map{
          case(id :: label :: features) =>
            val double_featues = features.map(v => v.toDouble).toArray
            (id, label, Vectors.dense(double_featues))
        }
        .toDF("id", "label", "features")

    val predictions = model.transform(features)

    predictions.select("id", "label", "predictedLabel").show()
  }
}
