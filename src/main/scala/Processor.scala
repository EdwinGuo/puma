package Puma

import java.io.Serializable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.mllib.clustering.{LDAModel, _}
import scala.util.{Success, Try}
import Puma.MathUtils._
import Puma.Constants._
import Puma.TimeUtil._
import Puma.Utils._

object Processor extends Serializable {

  /**
    * Filter DataFrame
    */
  private def filterRecords(inputProxyRecords: DataFrame): DataFrame = {

    val cleanProxyRecordsFilter = inputProxyRecords("p_date").isNotNull &&
    inputProxyRecords("p_time").isNotNull &&
    inputProxyRecords("clientip").isNotNull &&
    inputProxyRecords("host").isNotNull &&
    inputProxyRecords("fulluri").isNotNull

    inputProxyRecords
      .filter(cleanProxyRecordsFilter)
  }

  /**
    * Massage the initial input to the expected format
    *   |-- p_date: string (nullable = true)
    *   |-- p_time: string (nullable = true)
    *   |-- clientip: string (nullable = true)
    *   |-- host: string (nullable = true)
    *   |-- reqmethod: string (nullable = true)
    *   |-- useragent: string (nullable = true)
    *   |-- resconttype: string (nullable = true)
    *   |-- respcode: string (nullable = true)
    *   |-- fulluri: string (nullable = true)
    */
  def initialProxyInput(spark: SparkSession, file: String): DataFrame = {
    import spark.sqlContext.implicits._

    val df = spark.read.parquet(file)

    val s1 = df.select(expr("split(timestamp, 'T')[0]").as("p_date"), expr("split(timestamp, 'T')[1]").as("p_time"), $"cIp", $"csHost", $"csMethod", $"csUserAgent", $"rsContentType", $"scStatus", $"csUriPath", $"csUriQuery", $"csUriExtension").withColumn("fulluri", concat(when($"csHost".isNotNull, $"csHost").otherwise(lit("")), when($"csUriPath".isNotNull, $"csUriPath"), when($"csUriQuery".isNotNull, $"csUriQuery").otherwise(lit("")))).drop("csUriPath").drop("csUriQuery").drop("csUriExtension")

    val s2 = s1.withColumnRenamed("cIp", "clientip").withColumnRenamed("csHost", "host").withColumnRenamed("csMethod", "reqmethod").withColumnRenamed("csUserAgent", "useragent").withColumnRenamed("rsContentType", "resconttype").withColumnRenamed("scStatus", "respcode")

    filterRecords(s2).na.fill("-", Seq("useragent")).na.fill("-", Seq("resconttype"))
  }

  /**
    * Read, process and broadcast the top 1 million file
    * Params:
    *  top1m => Path on hdfs
    *
    */
  def returnTopMilWebsite(spark: SparkSession, top1m: String): Broadcast[Set[String]] = {
    val top1mData = spark.sparkContext.textFile(top1m).map(line => {
      val parts = line.split(",")
      parts(1).split('.')(0)
    }).collect.toSet

    spark.sparkContext.broadcast(top1mData)
  }

  /** Return a broadcast var of Map,
    * key: Agent name
    * value: Count
    * Example: AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.103 Safari/537.36" -> 44
    */
  def agentToCount(spark: SparkSession, df: DataFrame) = {
    val agentToCount: Map[String, Long] = df.select("useragent").rdd.map({ case Row(agent: String) => (agent, 1L) }).reduceByKey(_ + _).collect().toMap

    spark.sparkContext.broadcast(agentToCount)
  }


  /**
    * UDF for word creation
    *
    * @param topDomains  List of most popular top level domain names (provided)
    * @param agentCounts List of user agent values in the data set and its count
    * @return
    *  Example: [10.10.10.10, 1_0_CONNECT_13_-_22_4_200]
    */
  private def udfWordCreation(topDomains: Broadcast[Set[String]],
    agentCounts: Broadcast[Map[String, Long]]) =
    udf((host: String, time: String, reqMethod: String, uri: String, contentType: String, userAgent: String, responseCode: String) =>
      Try {
        List(topDomain(host, topDomains.value).toString,
          // Time binned by hours
          getTimeAsHour(time).toString,
          reqMethod,
          entropyBin(stringEntropy(uri), EntropyCuts),
          // Just the top level content type for now
          if (contentType.split('/').length > 0) contentType.split('/')(0) else "unknown_content_type",
          // Exponential cutoffs base 2
          logBaseXInt(agentCounts.value(userAgent), 2),
          // Exponential cutoffs base 2
          logBaseXInt(uri.length(), 2),
          // Response code using all 3 digits
          if (responseCode != null) responseCode else "unknown_response_code").mkString("_")
      } match {
        case Success(proxyWord) => proxyWord
        case _ => WordError
      }
    )

  /** return value can be reused by score function and returnDocWordCOunt
    *
    */
  def returnWordID(dataFrame: DataFrame, topDomains: Broadcast[Set[String]], agentCounts: Broadcast[Map[String, Long]]) = {
    val udfwc = udfWordCreation(topDomains, agentCounts)
    val df = dataFrame.withColumn("word", udfwc(
      dataFrame("host"),
      dataFrame("p_time"),
      dataFrame("reqmethod"),
      dataFrame("fulluri"),
      dataFrame("resconttype"),
      dataFrame("useragent"),
      dataFrame("respcode")
    ))
    df
  }

  /**
    * @Params df: result from udfWordCreation
    * Example of results:
    * LDAInput(172.29.1.11,0_1_CONNECT_14_-_22_4_200,22), LDAInput(172.22.142.13,1_1_POST_17_image_21_8_200,1)
    */
  def returnDocWordCount(spark: SparkSession, dataFrame: DataFrame): RDD[LDAInput] = {
    val df = dataFrame.select("clientip", "word")

    df.filter(df("word").notEqual(WordError))
      .rdd
      .map({ case Row(ip, word) => ((ip.asInstanceOf[String], word.asInstanceOf[String]), 1) })
      .reduceByKey(_ + _).map({ case ((ip, word), count) => LDAInput(ip, word, count) })
  }

  /** Return
    * wordDictionary: Map[String, Int] => Example: Map(1_0_GET_14_-_19_5_403 -> 18382)
    * where key is the word and value is the doc index
    * documentDictionary: DataFrame =>
    *  root
    *  |-- document_name: string (nullable = true)
    *  |-- document_number: long (nullable = false)
    *  Example:
    *  10.211.190.16|            0|
    *  10.38.6.74|              1|
    *  10.9.106.12|              2|
    */
  def returnLDAPreInput(spark: SparkSession, docWordCount: RDD[LDAInput]): (Map[String, Int], DataFrame) = {
    import spark.sqlContext.implicits._

    val docWordCountCache = docWordCount.cache()

    val wordDictionary: Map[String, Int] = {
      val words = docWordCountCache
        .map({ case LDAInput(_, word, _) => word })
        .distinct
        .collect
      words.zipWithIndex.toMap
    }

    val documentDictionary: DataFrame = docWordCount
      .map({ case LDAInput(doc, _, _) => doc })
      .distinct
      .zipWithIndex
      .toDF("document_name", "document_number")
      .cache

    (wordDictionary, documentDictionary)
  }

  def generateLDAInput(spark: SparkSession, wordDictionary: Map[String, Int], docWordCount: RDD[LDAInput], documentDictionary: DataFrame) = {
    import spark.sqlContext.implicits._

    val getWordId = {
      udf((word: String) => wordDictionary(word))
    }

    // --------------+--------------------+----------+
    // | document_name|           word_name|word_count|
    // +--------------+--------------------+----------+
    // | 172.29.167.87|0_1_CONNECT_14_-_...|        22|
    // |172.29.142.213|1_1_POST_17_image...|         1|
    // |   10.80.18.92|1_1_CONNECT_14_-_...|         2|
    val docWordCountDF = docWordCount.map({ case LDAInput(doc, word, count) => (doc, word, count) }).toDF("document_name", "word_name", "word_count")

    // +--------------+----------+---------------+-----------+
    // | document_name|word_count|document_number|word_number|
    // +--------------+----------+---------------+-----------+
    // | 172.29.167.87|        22|          11838|      15482|
    // |172.29.142.213|         1|          32443|       5210|
    // |   10.80.18.92|         2|          25407|      14322|
    // | 10.109.35.115|         4|          30448|      25686|
    val wordCountsPerDocDF = docWordCountDF.join(documentDictionary, docWordCountDF("document_name") === documentDictionary("document_name")).drop(documentDictionary("document_name")).withColumn("word_number", getWordId(docWordCountDF("word_name"))).drop("word_name")

    // (Long, Iterable[(Int, Double)]) = (77625,CompactBuffer((10996,2.0), (14322,2.0), (7051,1.0), (9519,2.0), (12682,2.0), (7491,2.0), (34172,1.0), (16205,2.0), (24293,1.0), (13190,2.0), (8874,1.0), (14878,2.0), (13874,1.0)))
    val wordCountsPerDoc: RDD[(Long, Iterable[(Int, Double)])] = wordCountsPerDocDF.select("document_number", "word_number", "word_count").rdd.map({ case Row(documentId: Long, wordId: Int, wordCount: Int) => (documentId.toLong, (wordId, wordCount.toDouble)) }).groupByKey
    val numUniqueWords = wordDictionary.size

    // Long => Documentid, Vector: (uniqueWords, Vec[wordId], Vec[wordCount])
    //(Long, org.apache.spark.mllib.linalg.Vector) = (77625,(35132,[7051,7491,8874,9519,10996,12682,13190,13874,14322,14878,16205,24293,34172],[1.0,2.0,1.0,2.0,2.0,2.0,2.0,1.0,2.0,2.0,2.0,1.0,1.0]))
    val ldaInput: RDD[(Long, Vector)] = wordCountsPerDoc.mapValues(vs => Vectors.sparse(numUniqueWords, vs.toSeq))
    ldaInput
  }

  /**
    * Format LDA output topicMatrix for scoring
    *
    * @param topicsMatrix LDA model topicMatrix
    * @return Map[String, Array[Double]]
    **/
  def formatTopicDistributions(topicsMatrix: Matrix, wordDictionary: Map[String, Int]): Map[String, Array[Double]] = {
    // Incoming word top matrix is in column-major order and the columns are unnormalized
    val m = topicsMatrix.numRows
    val n = topicsMatrix.numCols
    val reverseWordDictionary = wordDictionary.map(_.swap)

    val columnSums: Array[Double] = Range(0, n).map(j => Range(0, m).map(i => topicsMatrix(i, j)).sum).toArray

    val wordProbabilities: Seq[Array[Double]] = topicsMatrix.transpose.toArray.grouped(n).toSeq
      .map(unNormalizedProbabilities => unNormalizedProbabilities.zipWithIndex.map({ case (u, j) => u / columnSums(j) }))

    wordProbabilities.zipWithIndex
      .map({ case (topicProbabilities, wordInd) => (reverseWordDictionary(wordInd), topicProbabilities) }).toMap
  }


  /**
    * Format LDA output topicDistribution for scoring
    *
    * @param documentDistributions LDA model topicDistributions
    * @return DataFrame
    */
  def formatDocumentDistribution(spark: SparkSession, documentDistributions: RDD[(Long, Vector)], documentDictionary: DataFrame): DataFrame = {
    import spark.sqlContext.implicits._

    val topicDistributionToArray = udf((topicDistribution: Vector) => topicDistribution.toArray)

    val documentToTopicDistributionDF = documentDistributions.toDF("document_number", "topic_prob_mix")

    val documentToTopicDistributionArray = documentToTopicDistributionDF
      .join(documentDictionary, documentToTopicDistributionDF("document_number") === documentDictionary("document_number"))
      .drop(documentDictionary("document_number"))
      .drop(documentToTopicDistributionDF("document_number"))
      .select("document_name", "topic_prob_mix")
      .withColumn("topic_prob_mix_array", topicDistributionToArray(documentToTopicDistributionDF("topic_prob_mix")))
      .selectExpr(s"document_name  AS document_name", s"topic_prob_mix_array AS topic_prob_mix")

    documentToTopicDistributionArray
  }

  def score(documentTopicMix: Seq[Float], word: String, topicCount: Int, wordToPerTopicProbBC: Broadcast[Map[String, Array[Double]]]): Double = {

    val zeroProb = Array.fill(topicCount) {
      0d
    }

    if (word == "word_error") {
      -1.0
    } else {
      // If either the ip or the word key value cannot be found it means that it was not seen in training.
      val wordGivenTopicProbabilities = wordToPerTopicProbBC.value.getOrElse(word, zeroProb)
      val documentTopicMixDouble: Seq[Double] = toDoubles(documentTopicMix)

      documentTopicMixDouble.zip(wordGivenTopicProbabilities)
        .map({ case (pWordGivenTopic, pTopicGivenDoc) => pWordGivenTopic * pTopicGivenDoc })
        .sum
    }
  }

  def returnScoreResult(wordedDataFrame: DataFrame, ipToTopicMix: DataFrame, topicCount: Int, wordToPerTopicProbBC: Broadcast[Map[String, Array[Double]]]) = {
    def udfScoreFunction = udf((documentTopicMix: Seq[Float], word: String) =>
      score(documentTopicMix, word, topicCount, wordToPerTopicProbBC))

    wordedDataFrame
      .join(org.apache.spark.sql.functions.broadcast(ipToTopicMix), wordedDataFrame("clientip") === ipToTopicMix("document_name"), "left_outer")
      .selectExpr(wordedDataFrame.schema.fieldNames :+ "topic_prob_mix": _*)
      .withColumn("score", udfScoreFunction(col("topic_prob_mix"), col("word")))
      .drop("topic_prob_mix")
  }

  /**
    *
    * @param scoredProxyRecords scored proxy records.
    * @param threshold          score tolerance.
    * @return
    */
  def filterScoredRecords(scoredProxyRecords: DataFrame, threshold: Double): DataFrame = {

    val filteredProxyRecordsFilter = scoredProxyRecords("score").leq(threshold) &&
    scoredProxyRecords("score").gt("-1.0")

    scoredProxyRecords.filter(filteredProxyRecordsFilter)
  }
}
