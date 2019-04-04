package com.examplecompany.sales.GraphFrames

import com.examplecompany.sales.DataSet
import org.graphframes.GraphFrame

/**
  * Top-level class for the K-Means Clustering algorithm.
  * Handles training models and creating sessions for recommendations based on the data set passed in.
  */
class GraphFramesSuggester(dataSet : DataSet) {
  private val _graphFrame = createGraphFrame(dataSet)

  def createSession(dataSet: DataSet) : GraphFramesSession = new GraphFramesSession(_graphFrame)

  def createGraphFrame(dataSet : DataSet) : GraphFrame = {
    val customers = dataSet.customers.selectExpr("CUSTOMER_NO as id", "BILL_TO_NAME as name")
    val items = dataSet.sellStyles.selectExpr("CONCAT(SELLING_STYLE_NO, ',', CUST_REF_NO) as id", "SELLING_STYLE_NAME as name")
    val purchases = dataSet.invoices.selectExpr("CUSTOMER_NO as src", "CONCAT(SELLING_STYLE_NO, ',', CUST_REF_NO) as dst", "Expenditure as weight")
    val graph = GraphFrame.apply(customers.unionAll(items), purchases)
    graph
  }
}

object GraphFramesSuggester {
}