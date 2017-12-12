package Puma

object Constants {
  /**
    * Spot LDA input case class
    *
    * @param doc   Document name.
    * @param word  Word.
    * @param count Times the word appears for the document.
    */
  case class LDAInput(doc: String, word: String, count: Int) extends Serializable

  // These buckets are optimized to data sets used for training. Last bucket is of infinite size to ensure fit.
  // The maximum value of entropy is given by log k where k is the number of distinct categories.
  // Given that the alphabet and number of characters is finite the maximum value for entropy is upper bounded.
  // Bucket number and size can be changed to provide less/more granularity
  val EntropyCuts = Array(0.0, 0.3, 0.6, 0.9, 1.2,
    1.5, 1.8, 2.1, 2.4, 2.7,
    3.0, 3.3, 3.6, 3.9, 4.2,
    4.5, 4.8, 5.1, 5.4, Double.PositiveInfinity)

  val WordError = "word_error"
  val ScoreError = -1d
}
