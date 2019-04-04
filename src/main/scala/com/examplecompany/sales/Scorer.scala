package com.examplecompany.sales

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Base class which determines what the score of a given session is.
  * This is used during performance tuning in order to rank different parameters in terms of how effective
  * a given set of parameters (represented by the session) is.
  */
class Scorer(dataSet : DataSet) {
  private val sellStyleData = initSellStyleData(dataSet)
  private val sellStylesBought = initSellStylesBought(dataSet)
  private val output = new AnalysisRecommendationOutput(dataSet)

  /**
    * Initialize the sell style data based on the raw data set.
    * @param dataSet The raw data set to query from.
    * @return The sell style data that is necessary for our operations.
    */
  private def initSellStyleData(dataSet : DataSet) = {
    dataSet.sellStyles.map(r => (r.getAs[String]("SELLING_STYLE_NO"), (r.getAs[String]("PROD_DESC"), r.getAs[String]("CHO_SUB_CAT"), r.getAs[String]("CPT_HDS_OTH")))).persist(StorageLevel.MEMORY_AND_DISK)
  }

  /**
    * Initialize the styles which have been bought by the customers we are interested in scoring.
    * @param dataSet The data set to pull information from.
    * @return A map from customer ID to a list of styles which have been bought by that customer.
    */
  private def initSellStylesBought(dataSet : DataSet) = {
    val customerData = dataSet.invoices.map(r => (r.getAs[String]("SELLING_STYLE_NO"), r.getAs[Integer]("CUSTOMER_NO")))
    val joinedData = customerData.join(sellStyleData).cache()

    // DTODO: Is there a better way to check more customers per configuration?
    // Currently, this represents all of the customers which we are using for testing, so we get the best results possible.
    val customerIDs = Array.apply(56375, 145562, 86294, 158093, 6023416, 223114, 110191, 34519, 66728, 133451, 129022)
    val map = customerIDs.map(c => {
      val boughtItems = joinedData.filter{case (_, (customerID, _)) => c == customerID}.map{case (_, (_, d)) => d}
      (c, boughtItems.distinct().collect())
    }).toMap
    map
  }

  /**
    * Gets the full score for a given session.
    * @param suggesterSession The session to score.
    * @param isMoney Whether the score that comes back will be in terms of money or not.
    * @return A string representing the score for this session.
    */
  def getFullScoreForCluster(suggesterSession : RecommenderSession, isMoney : Boolean) : String = {
    val score = sellStylesBought.keys.toList.map(c => {
      val suggestions = suggesterSession.getSuggestions(c)
      getRecommendationScoreForCustomer(suggestions, c)
    }).sum / sellStylesBought.size.toDouble
    new java.text.DecimalFormat("0.#####").format(score) + "%"
  }

  /**
    * Gets the score for a given customer for a given session.
    * @param itemPairPercentages Recommendations for that customer.
    * @param customerID The customer ID we are analyzing.
    * @return A double indicating the score for the session for the given customer.
    */
  private def getRecommendationScoreForCustomer(itemPairPercentages: RDD[(String, Double)], customerID : Integer) : Double = {
    val topSuggestions = output.reportSuggestions(itemPairPercentages, customerID, isMoney=false).takeOrdered(10)(Ordering.by[(String, Double), Double](x => -x._2))
    val topSuggestedItems = sellStyleData.filter{case (s, _) => topSuggestions.exists(x => x._1 == s)}.
      reduceByKey((a, _) => a).cache()

    val topSuggestedProductDescriptions = topSuggestedItems.map{ case (_, info) => info}

    val itemsBought = sellStylesBought.apply(customerID)
    val productDescriptionsBought = itemsBought.map{ case (d, _, _) => d}
    val subcategoriesBought = itemsBought.map{ case (_, sc, _) => sc}
    val highLevelType = itemsBought.map{ case (_, _, t) => t}

    val rawScore = topSuggestedProductDescriptions.map{ case (d, sc, t) =>
      if (productDescriptionsBought.contains(d)) 1.0 else
      if (subcategoriesBought.contains(sc)) 0.75 else
      if (highLevelType.contains(t)) 0.5 else 0
    }.sum()

    rawScore * 100.0 / topSuggestions.length.toDouble
  }
}
