package com.examplecompany.sales.GraphFrames

import com.examplecompany.sales.RecommenderSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.graphframes.GraphFrame

class GraphFramesSession(private val _graph : GraphFrame) extends java.io.Serializable with RecommenderSession {
  /**
    * Returns the recommendations for the given customer.
    *
    * @param customerID ID for the customer to be queried.
    * @return An RDD of tuples of selling style number and score. The scores aren't necessarily sorted.
    *         Highest score means the best suggestion.
    */
  override def getSuggestions(customerID: Integer): RDD[(String, Double)] = {
    val allCommonEdges = _graph.find("(a)-[e]->(b); (c)-[e2]->(b)")
    val commonEdges = allCommonEdges.filter(allCommonEdges("a.id")===customerID)
    val keyedWeights = commonEdges.map(r => {
      val c = r.getAs[Row]("c")
      val e = r.getAs[Row]("e")
      val e2 = r.getAs[Row]("e2")
      (c.getAs[String]("id"), e.getAs[Double]("weight") * e2.getAs[Double]("weight"))
    })
    val comparisonScoresNumerators = keyedWeights.reduceByKey{ case (left, right) => left + right}

    val allPurchasedItems = _graph.find("(a)-[e]->()").selectExpr("a.id as id", "POW(e.weight, 2) as weight")
    val comparisonScoresDenominators = allPurchasedItems.groupBy("id").sum("weight")
    val collectedDenominators = comparisonScoresDenominators.map(r => (r.getAs[String]("id"), r.getAs[Double]("sum(weight)"))).collectAsMap()

    val comparisonScores = comparisonScoresNumerators.map{ case (id, top) =>
      (id, top / (math.sqrt(collectedDenominators(customerID.toString)) * math.sqrt(collectedDenominators(id))))}

    val boughtItems = _graph.find("(a)-[e]->(b)").map(r => {
      val a = r.getAs[Row]("a")
      val b = r.getAs[Row]("b")
      val e = r.getAs[Row]("e")
      (a.getAs[String]("id"), (b.getAs[String]("id"), e.getAs[Double]("weight")))
    })
    comparisonScores.join(boughtItems).map{ case (_, (score, (item, expenditure))) => (item.split(",")(0), expenditure * score)}
  }
}
