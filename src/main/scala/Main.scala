package Puma

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.mllib.clustering.{LDAModel, _}
import scala.util.{Success, Try}
import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.mllib.clustering.{LDAModel, _}
import Puma.Processor._
import Puma.Constants._
import Puma.Utils._
import Puma.ModelLDA

/**
  *
  */
object Main extends Serializable {

  val spark = SparkSession
    .builder()
    .appName("PROXY LDA")
    .getOrCreate()

  val file = "file:///file"

  val top1m = "/top-1m.csv"

  val df = initialProxyInput(spark, file)

  val topDomains = returnTopMilWebsite(spark, top1m)

  val agentToCountBC = agentToCount(spark, df)

  val wordedDataFrame = returnWordID(df, topDomains, agentToCountBC)

  val docWordCount: RDD[LDAInput] = returnDocWordCount(spark, wordedDataFrame)

  val (wordDictionary, documentDictionary): (Map[String, Int], DataFrame) = returnLDAPreInput(spark, docWordCount)

  val ldaCorpus = generateLDAInput(spark, wordDictionary, docWordCount, documentDictionary)

  // Actually learning
  // pick onlineLdaoptimizer for start,
  // TODO, EMLDAoptimizer

  // Hyperparameter
  val topicCount = 20
  val  maxIterations = 1000
  val ldaAlpha = 0.5
  val ldaBeta = 0.5

  val ldaOptimizer = new OnlineLDAOptimizer().setOptimizeDocConcentration(true).setMiniBatchFraction({
    val corpusSize = ldaCorpus.count()
    if (corpusSize < 2) 0.75
    else (0.05 + 1) / corpusSize
  })

  val lda =new LDA()
    .setK(topicCount)
    .setMaxIterations(maxIterations)
    .setAlpha(ldaAlpha)
    .setBeta(ldaBeta)
    .setOptimizer(ldaOptimizer)

  val model: LDAModel = lda.run(ldaCorpus)

  val (topicDistributions, topicsMatrix) = ModelLDA.predict(spark, model, ldaCorpus)

  val ipToTopicMix: DataFrame = formatDocumentDistribution(spark, topicDistributions, documentDictionary)

  val wordToPerTopicProb: Map[String, Array[Double]] = formatTopicDistributions(topicsMatrix, wordDictionary)

  val wordToPerTopicProbBC = spark.sparkContext.broadcast(wordToPerTopicProb)

  val scoreResult = returnScoreResult(wordedDataFrame, ipToTopicMix, topicCount, wordToPerTopicProbBC)

  val filteredScored = filterScoredRecords(scoreResult, 0.5)

  val orderedProxyRecords = filteredScored.orderBy("score")


}
