package com.examplecompany.sales
import org.apache.spark.rdd.RDD

/**
  * Output used in parameter tuning and scoring scenarios.
  * This output will only run the necessary pre-processing steps, and will not actually record the results anywhere,
  * instead just returning them to the program.
  */
class AnalysisRecommendationOutput(private val dataSet : DataSet) extends RecommendationOutput {
  override def setTechnique(technique: String): String = { "" }

  override def reportSuggestions(itemPairPercentages: RDD[(String, Double)], customerID: Integer, isMoney: Boolean): RDD[(String, Double)] = {
    RunPreProcessing(itemPairPercentages, customerID, dataSet, -1)
  }
}
