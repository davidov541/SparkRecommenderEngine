package com.examplecompany.sales

import org.apache.spark.rdd.RDD

/**
  * Trait which represents a given recommender session.
  * A session should have all of the training and pre-processing completed,
  * in order to handle purely queries to that model.
  */
trait RecommenderSession {
  /**
    * Returns the recommendations for the given customer.
    * @param customerID ID for the customer to be queried.
    * @return An RDD of tuples of selling style number and score. The scores aren't necessarily sorted.
    *         Highest score means the best suggestion.
    */
  def getSuggestions(customerID: Integer) : RDD[(String, Double)]
}
