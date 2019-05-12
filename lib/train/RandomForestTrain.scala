import java.io.PrintWriter

import cc.xmccc.sparkdemo.schema.HBaseOpsUtil._
import cc.xmccc.sparkdemo.schema._
import it.nerdammer.spark.hbase._
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

object RandomForestMlTest {

  val csvFile = new PrintWriter("result.csv")

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("RandomForest")
      .getOrCreate()

    val inputTable = args(0) //HBase table for storing the session features
    val trees_start = args(1).toInt
    val trees_stop = args(2).toInt
    val trees_step = args(3).toInt
    val depth_start = args(4).toInt
    val depth_stop = args(5).toInt
    val model_path = args(6) //path to save the random forest model in HDFS

    val input_rdd = sparkSession.sparkContext.hbaseTable[(Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[String], Array[Double], String)](inputTable)
      .select("sport", "dport", "direction", "m", "sid", "features_name", "features_value", "dataset")
      .inColumnFamily("sessn")
      .map{
        case(rowkey, sport, dport, direction, m, sid, features_name, features_value, dataset) =>
          (NewSessionFeatureTable(
            rowkey, sport, dport, direction, m, sid, features_name, features_value
          ), dataset)
      }

    import sparkSession.implicits._

    val alldata_rdd = input_rdd.map{
      case(table, dataset) =>
        (dataset, table.features_value)
    }

    println(alldata_rdd.count())


    val datas = alldata_rdd.map{
      case(dataset, features) =>
        val feature_vec = Vectors.dense(features)
        (dataset, feature_vec)
    }.toDF("label", "features")

    for {
      t <- trees_start to trees_stop by trees_step
      d <- depth_start to depth_stop
    } {
      val last = System.currentTimeMillis()
      val (model, result, labels) = executeJob(datas, t, d)
      val now = System.currentTimeMillis()

      csvFile.println("id," + "all," + labels.mkString(",") + ",time")
      csvFile.println(result + s",${now - last}")

      model.save(model_path) //save model
      model.stages(1).asInstanceOf[RandomForestClassificationModel].save(model_path + "/randomforest")

      val randomforest_model = model.stages(1).asInstanceOf[RandomForestClassificationModel]

      new PrintWriter(s"trees/${t}-${d}.tree") {
        println(randomforest_model.toDebugString)
        close()
      }
    }

    sparkSession.close()
    csvFile.close()

  }

  def executeJob(datas: DataFrame, trees: Integer, depth: Integer): (PipelineModel, String, List[String]) = {

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(datas)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val labels = labelIndexer.labels.toList
    val Array(trainingData, testdata) = datas.randomSplit(Array(0.8, 0.2))

    println(trainingData.count())

    System.out.println(labelIndexer.labels.toList.mkString(","))

    val numClasses = labels.length
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = trees
    val featureSubsetStrategy = "auto"
    val impurity = "gini"
    val maxDepth = depth
    val maxBins = 32

    val rf = new RandomForestClassifier()
      .setFeatureSubsetStrategy("auto")
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features")
      .setNumTrees(numTrees)
      .setImpurity(impurity)
      .setMaxDepth(maxDepth)

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, rf, labelConverter))

    val model = pipeline.fit(trainingData)

    val predictions = model.transform(testdata)

    val labelPredicts = predictions.select("label", "predictedLabel")

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)

    val label_accurancy = labelPredicts
        .rdd.map(row => (row.getString(0), row.getString(1)))
        .groupBy(_._1).map{
          case(label, preds) =>
          val preds_list = preds.toList
          (label, preds_list.filter(x => x._1 == x._2).length.toDouble / preds_list.length.toDouble)
        }.collect.toMap

    val result = accuracy.toString +: labels.map{
      l =>
        label_accurancy.get(l) match {
          case Some(v) => v.toString
          case _ => "NaN"
        }
    }

    (model, s"${trees}-${depth}," + result.mkString(","), labels)
  }
}
