package com.examplecompany.sales.CF

import com.examplecompany.sales.{App, DataSet}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.sql.DataFrame

class CFSuggester(private val dataSet : DataSet) extends java.io.Serializable {
  private var ratings : Option[CFRatings] = None
  private val (sellingStyleLookup, reverseSellingStyleLookup) = App.createLookup(dataSet.invoices.select("SELLING_STYLE_NO").map(_.getAs[String](0)))
  private val fullTable = CFSuggester.getFullTable(dataSet)

  private def getRatings = if (ratings.isDefined) ratings.get else { ratings = Some.apply(new CFRatings(dataSet, fullTable)); ratings.get}

  def createSession(rank : Integer = 50, numIterations : Integer = 10, lambda : Double = 0.001, seed : Option[Long] = None) : CFSession = {
    val ratings = getRatings.scaled

    val model = if(seed.isDefined) ALS.train(ratings, rank, numIterations, lambda, -1, seed.get) else ALS.train(ratings, rank, numIterations, lambda)
    new CFSession(model, sellingStyleLookup, reverseSellingStyleLookup, fullTable)
  }

  def createImplicitSession(rank : Integer = 200, numIterations : Integer = 10, lambda : Double = 0.01, alpha : Double = 0.01, seed : Option[Long] = None) : CFSession = {
    val model = if(seed.isDefined) ALS.trainImplicit(getRatings.unscaled, rank, numIterations, lambda, -1, alpha, seed.get) else ALS.trainImplicit(getRatings.unscaled, rank, numIterations, lambda, alpha)
    new CFSession(model, sellingStyleLookup, reverseSellingStyleLookup, fullTable)
  }
}

object CFSuggester {
  def apply(dataSet : DataSet) : CFSuggester = {
    new CFSuggester(dataSet)
  }

  def getFullTable(dataSet : DataSet) : DataFrame = {
    dataSet.invoices.select("CUSTOMER_NO", "SELLING_STYLE_NO", "CUST_REF_NO", "QUANTITY", "UNIT_PRICE").cache()
  }
}
