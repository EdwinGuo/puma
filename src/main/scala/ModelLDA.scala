package Puma

import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDAModel, LocalLDAModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors}

object ModelLDA {

  def save(spark: SparkSession, ldaModel: LDAModel, location: String) {
    ldaModel.save(spark.sparkContext, location)
  }

  def predict(spark: SparkSession, ldaModel: LDAModel, ldaCorpus: RDD[(Long, Vector)]) = {
    import spark.sqlContext.implicits._

    ldaModel match {
      case model: DistributedLDAModel => {val localLDAModel: LocalLDAModel = ldaModel.asInstanceOf[LocalLDAModel]

        val topicDistributions = localLDAModel.topicDistributions(ldaCorpus)
        val topicMix = localLDAModel.topicsMatrix

        (topicDistributions, topicMix)}

      case model: LocalLDAModel => {
        val localLDAModel: LocalLDAModel = ldaModel.asInstanceOf[LocalLDAModel]

        val topicDistributions = localLDAModel.topicDistributions(ldaCorpus)
        val topicMix = localLDAModel.topicsMatrix

        (topicDistributions, topicMix)
      }
    }


  }



}
