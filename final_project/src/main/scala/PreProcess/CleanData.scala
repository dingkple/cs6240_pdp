package PreProcess

import java.util

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Created by kingkz on 11/30/16.
  */

object CleanData {
    val sourceFile = "/Users/kingkz/Downloads/labeled_2.csv"
    val sampelFile = "randomchosen.csv"

    val skip = (Seq(1, 2, 3, 4, 9, 10, 11, 12, 16, 18) ++ (1017 to 1019) ++
        (20 to 26) ++ (28 to 963)).toSet
    val cate = ((964 to 966) ++ (1020 to 1084)).toSet
    val labelIdx = 27
    val oneToK = false

    def getSampleFile(sc: SparkContext, fs: FileSystem): Unit = {

        val random = new Random(System.nanoTime())
        val output = new Path(sampelFile)
        if (fs.exists(output)) {
            fs.delete(output, true)
        }
        val sampleRows = sc.textFile(sourceFile)
            .filter(row => {
                if (row.startsWith("SAMPLING_EVENT_ID")) false
                else random.nextDouble() < math.pow(10, -2)
            })

        sampleRows.repartition(1).saveAsTextFile(output.toString)
    }

    def generateCleanData(sc: SparkContext, data: RDD[String]): RDD[LabeledPoint] = {
        val data = sc.textFile(sampelFile)
            .map(row => {
                val buffer = new ListBuffer[Double]
                val withIndex = row.split(",").zipWithIndex
                assert(withIndex.length == 1657)
                var label = 0.0
                withIndex.foreach{

                    case(col, i) => {
//                        println(col, i)
                        if (skip.contains(i+1)) buffer += 0.0
                        else if (cate.contains(i+1)) {
                            var value = 0
                            if (col != "?") value = col.toInt
                            value = math.max(0, value)
                            value = math.min(9, value)
                            if (oneToK) {
                                val expand: Array[Double] = new Array[Double](10)

                                if (value > -1)
                                    expand(value) = 1.0

                                buffer ++= expand
                            } else {
                                if (-1 < value && value < 10)
                                    buffer += value
                                else
                                    buffer += 0.0
                            }

                            if (i == 1083) println("im done", value, i)

                        } else if (i+1 == labelIdx) {
                            if (col != "0") label = 1.0
                            buffer += 0.0
                        } else {
                            if (col != "?")
                                buffer += col.toDouble
                            else
                                buffer += 0.0
                        }
                    }
                }
                println("row", label)
                LabeledPoint.apply(label, Vectors.dense(buffer.toArray))
            })
        data
    }

    def decisionTreeTest(cleaned: RDD[LabeledPoint]) : Unit = {

        val splits = cleaned.randomSplit(Array(0.8, 0.2))
        val training = splits(0).cache()
        val test = splits(1).cache()

        val numTraining = training.count()
        val numTest = test.count()
        println(s"Training: $numTraining, test: $numTest.")
        var categoricalInfo = Map[Int, Int]()
        for (k <- cate) {
            categoricalInfo += (k-1) -> 10
        }

        val numClasses = 2
        val impurity = "gini"
        val maxDepth = 5
        val maxBins = 32

        val decisionTree = DecisionTree.trainClassifier(
            training,
            numClasses,
            categoricalInfo,
            impurity,
            maxDepth,
            maxBins
        )
        // Evaluate model on test instances and compute test error
        val labelAndPreds = test.map { point =>
            val prediction = decisionTree.predict(point.features)
            (point.label, prediction)
        }
        val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / test.count()
        println("Test Error = " + testErr)
        println("Learned classification tree model:\n" + decisionTree.toDebugString)
    }

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        val hadoopConf = new org.apache.hadoop.conf.Configuration()
        val fileSystem = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
        conf.setMaster("local[*]")
            .setAppName("final_project")

        val sc = new SparkContext(conf)

        getSampleFile(sc, fileSystem)
        val cleaned = generateCleanData(sc, sc.textFile(sampelFile))

        val splits = cleaned.randomSplit(Array(0.8, 0.2))
        val training = splits(0).cache()
        val test = splits(1).cache()

        val numTraining = training.count()
        val numTest = test.count()
        println(s"Training: $numTraining, test: $numTest.")

        cleaned.unpersist(blocking = false)

//        val updater = params.regType match {
//            case L1 => new L1Updater()
//            case L2 => new SquaredL2Updater()
//        }
        val updater = new L1Updater()

//        val model = params.algorithm match {
//            case LR =>
//                val algorithm = new LogisticRegressionWithLBFGS()
//                algorithm.optimizer
//                    .setNumIterations(12)
//                    .setUpdater(updater)
//                    .setRegParam(1.0)
//                val model = algorithm.run(training).clearThreshold()
//            case SVM =>
//                val algorithm = new SVMWithSGD()
//                algorithm.optimizer
//                    .setNumIterations(12)
//                    .setStepSize(0.1)
//                    .setUpdater(updater)
//                    .setRegParam(1.0)
//                val model = algorithm.run(training).clearThreshold()
//        }

//
//
//        val prediction = model.predict(test.map(_.features))
//        val predictionAndLabel = prediction.zip(test.map(_.label))
//
//        val metrics = new BinaryClassificationMetrics(predictionAndLabel)
//
//        println(s"Test areaUnderPR = ${metrics.areaUnderPR()}.")
//        println(s"Test areaUnderROC = ${metrics.areaUnderROC()}.")
//        println(s"Test precision = ${metrics.pr()}.")

        decisionTreeTest(cleaned)
        sc.stop()
    }
}