package com.examplecompany.sales

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types._

/**
  * Output class which inserts the recommendation into a Hive table.
  */
class HiveRecommendationOutput(rawDataSet : DataSet, private val hiveContext : HiveContext,
                               private val expirationMonths : Integer,
                               private val mixesToExclude : Array[String]) extends RecommendationOutput with java.io.Serializable {
  private var tableName = "Recommendations"

  def setTechnique(technique: String) : String = { "" }

  def setTableName(tableName : String) : String = { this.tableName = tableName; tableName }

  def reportSuggestions(itemPairPercentages : RDD[(String, Double)], customerID : Integer, isMoney : Boolean): RDD[(String, Double)] = {
    val validSuggestions = RunPreProcessing(itemPairPercentages, customerID, rawDataSet, expirationMonths)
    val mixes = rawDataSet.mixes.map(r => (r.getAs[String]("SellStyleNo"), r.getAs[String]("Mix")))
    val validSellStyles = mixes.mapValues{ m => Array.apply(m)}.reduceByKey((x, y) => x ++ y).
      filter{ x => mixesToExclude.intersect(x._2).isEmpty}
    val suggestions = validSuggestions.join(validSellStyles).reduceByKey((x, _) => x).map{ case (i, (score, _)) => (score, i)}.
      sortByKey(ascending = false).take(20)

    val rowType = new StructType(Array.apply(StructField.apply("CustomerID", IntegerType, nullable = false),
      StructField.apply("SellingStyleNo", StringType, nullable = false), StructField.apply("Score", DoubleType, nullable = false)))
    val predictionRDD = hiveContext.sparkContext.parallelize(suggestions).map {
      case (score : Double, item : String) => Row.apply(customerID, item, score) }
    val predictionDataFrame = hiveContext.createDataFrame(predictionRDD, rowType)
    predictionDataFrame.write.mode(SaveMode.Append).saveAsTable(tableName)
    
    validSuggestions
  }
}
