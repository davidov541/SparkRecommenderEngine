package com.examplecompany.sales.CF

import com.examplecompany.sales.{App, DataSet, RecommenderSession}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.immutable.Map

class CFSession(private val model : MatrixFactorizationModel, sellingStyleLookup : Map[String, Int], reverseSellingStyleLookup : Map[Int, String], fullTable : DataFrame) extends java.io.Serializable with RecommenderSession {
  private val tablePair = fullTable.map(r => (sellingStyleLookup.apply(r.getAs[String](1)), r.getAs[Int](0))).cache()
  private val allItemsBought = tablePair.map(_._1).distinct().cache()

  def getSuggestions(customerID: Integer) : RDD[(String, Double)] = {
    val possibleCustomersPurchases: RDD[(Int, Int)] = allItemsBought.map(item => (customerID, item))
    val prediction = model.predict(possibleCustomersPurchases).map { case Rating(_, product, rate) => (reverseSellingStyleLookup.apply(product), rate) }.cache()
    prediction
  }

  def saveModel(sc : SparkContext, saveLocation : String) { model.save(sc, saveLocation) }
}
object CFSession {
  def apply(sc : SparkContext, modelLocation : String, dataSet : DataSet) : CFSession = {
    val model = MatrixFactorizationModel.load(sc, modelLocation)
    val (sellingStyleLookup, reverseSellingStyleLookup) = App.createLookup(dataSet.invoices.select("SELLING_STYLE_NO").map(_.getAs[String](0)))
    val fullTable = CFSuggester.getFullTable(dataSet)
    new CFSession(model, sellingStyleLookup, reverseSellingStyleLookup, fullTable)
  }
}
