package com.examplecompany.sales

import org.apache.spark.rdd.RDD

/**
  * Output class which writes the recommendations to the command line on which it is running.
  * This should be used only for testing scenarios in order to ensure recommendations are being found correctly.
  */
class CommandLineRecommendationOutput(private val rawDataSet : DataSet, private val expirationMonths : Integer, private val mixesToExclude : Array[String]) extends RecommendationOutput with java.io.Serializable {
  private var technique = "NA"

  def setTechnique(technique: String) : String = { this.technique = technique; technique;}

  def reportSuggestions(itemPairPercentages: RDD[(String, Double)], customerID: Integer, isMoney : Boolean): RDD[(String, Double)] = {
    val validSuggestions = RunPreProcessing(itemPairPercentages, customerID, rawDataSet, expirationMonths)

    val filteredSellStyle = rawDataSet.sellStyles.map(r => (r.getAs[String]("SELLING_STYLE_NO"), (r.getAs[String]("SELLING_STYLE_NAME"), r.getAs[String]("PROD_DESC"),
      r.getAs[String]("CPT_HDS_OTH"), r.getAs[String]("CHO_SUB_CAT"))))
    val mixes = rawDataSet.mixes.map(r => (r.getAs[String]("SellStyleNo"), r.getAs[String]("Mix")))
    val fullSellStyle = filteredSellStyle.leftOuterJoin(mixes).mapValues{ case ((n, d, t, c), m) => (n, d, t, c, Array.apply(m.getOrElse("null")))}.reduceByKey((x, y) => (x._1, x._2, x._3, x._4, x._5 ++ y._5)).
      filter{ x => mixesToExclude.intersect(x._2._5).isEmpty}
    val fullSuggestionInfo = validSuggestions.join(fullSellStyle).reduceByKey((x, _) => x).map { case (i, (r, (n, d, t, c, m))) =>
      val mixStr = if (m.distinct.length == 1 && m.distinct.apply(0) == "null") "" else m.distinct.mkString(",")
      (r, (i, r, n, d, c, t, mixStr))
    }
    val topTwentySuggestions = fullSuggestionInfo.sortByKey(ascending = false).values.take(20)
    val customerName = rawDataSet.customers.filter(f"CUSTOMER_NO = $customerID").select("BILL_TO_NAME").take(1).apply(0).apply(0).toString
    var result = "Suggestions for " + customerName + " by " + technique + "\n"
    result += "----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n"
    for (suggestion <- topTwentySuggestions) {
      result += f"${suggestion._1.toString}%-7s" + "\t" + f"${suggestion._3}%-25s" + "\t" + f"${suggestion._4}%-30s" + "\t" +
        f"${suggestion._5}%-30s" + "\t" + f"${suggestion._6}%-15s" + "\t" + f"${suggestion._7}%-40s" + "\t"
      result += (if (isMoney) { "$" + f"${suggestion._2}%-20.2f" + "\n" } else { f"${suggestion._2}%-20.4f" + "\n"})
    }
    println(result)
    validSuggestions
  }
}
