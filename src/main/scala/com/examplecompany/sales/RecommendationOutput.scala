package com.examplecompany.sales

import org.apache.spark.rdd.RDD

import java.time.LocalDate

/**
  * Trait describing how to output recommendations that have been reached.
  * Contains functions to pre-process the recommendations based on restrictions (such as mixes to exclude, etc),
  * as well as a few functions to control how it is output.
  */
trait RecommendationOutput {

  /**
    * Goes through the recommendations, removing recommendations that are
    * not to be displayed to the user based on invoice date and mix.
    * @param itemPairPercentages Recommendations to sort through and filter.
    * @param customerID The customer ID these recommendations are for.
    * @param rawDataSet The raw dataset from which these recommendations came from.
    * @param expirationMonths How many months ago a style must have been bought by the
    *                         customer before it will be displayed as a recommendation.
    *                         -1 indicates that all styles bought by the customer ever will be ignored.
    * @return An RDD of recommendations which have been filtered.
    */
  protected def RunPreProcessing(itemPairPercentages: RDD[(String, Double)], customerID: Integer, rawDataSet : DataSet, expirationMonths : Integer) : RDD[(String, Double)] = {
    val itemsBoughtByCustomer = rawDataSet.invoices.filter("CUSTOMER_NO = " + customerID.toString).select("SELLING_STYLE_NO", "INVOICE_DATE").map(r => (r.getAs[String]("SELLING_STYLE_NO"), r.getAs[String]("INVOICE_DATE")))
    val lastValidDate = LocalDate.now().minusMonths(expirationMonths.toLong)
    val validSuggestions = itemPairPercentages.leftOuterJoin(itemsBoughtByCustomer).filter {
      case (_, (_, None)) => true
      case (_, (_, Some(d))) => if (expirationMonths > 0) LocalDate.parse(d).compareTo(lastValidDate) < 0 else false
    }.map { case (item, (score, _)) => (item, score) }
    validSuggestions
  }

  /**
    * Sets the name of the technique which is being used to generate these recommendations.
    * This is mainly used for display purposes.
    * @param technique The name of the technique to display to the user.
    * @return The technique name is returned back.
    */
  def setTechnique(technique: String) : String

  /**
    * Report the recommendations in a way that is suitable to the user. The recommendations will be sorted by most to least
    * likely recommendation.
    * @param itemPairPercentages Recommendations to display.
    * @param customerID Customer ID for which these recommendations were generated.
    * @param isMoney Whether the score is in terms of dollar amount or not.
    * @return The recommendations are returned back.
    */
  def reportSuggestions(itemPairPercentages: RDD[(String, Double)], customerID: Integer, isMoney: Boolean): RDD[(String, Double)]
}
