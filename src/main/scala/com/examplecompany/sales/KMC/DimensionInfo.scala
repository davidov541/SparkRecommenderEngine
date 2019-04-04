package com.examplecompany.sales.KMC

import com.examplecompany.sales.{App, DataSet}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
  * Contains all dimensions for all customers. Normalizes the data and allows for querying by customer.
  */
class DimensionInfo(private val dataSet : DataSet, private var dimensions : Array[String]) extends java.io.Serializable {
  private val possibleCustomerColumnValues = collection.mutable.Map[String, Array[String]]()

  /**
    * Adds a column to the list of columns which we are tracking all possible values for.
    * Should only be used during initialization.
    * @param columnName The name of the column to track.
    * @return The new mapping of column names to all possible values for that column.
    */
  private def addCustomerColumnValues(columnName : String) = {
    possibleCustomerColumnValues += (columnName -> dataSet.customers.select(columnName).map(r => {
      val value = r.get(0)
      if (value == null) "" else value.toString
    }).distinct().collect())
  }
  addCustomerColumnValues("channel")
  addCustomerColumnValues("SHWONL_FLAG")
  addCustomerColumnValues("CREDIT_TYPE")

  private val allDimensionInfo = getAllDimensionInfo
  private val stats = DimensionalStats.apply(allDimensionInfo.map(x => getLabelledDimensions(x._2, DimensionInfo.allDimensions)))

  /**
    * Initializes values for the dimensions requested, including obtaining values and normalizing them.
    * @param dimensions List of dimensions to get.
    * @return An RDD mapping of customer IDs to vectors representing those dimensions.
    */
  private def initDimensions(dimensions : Array[String]) : RDD[(Integer, Vector)] = {
    this.dimensions = dimensions
    val nonNormalizedValues = allDimensionInfo.map(x => (x._1, getLabelledDimensions(x._2, dimensions, Some.apply(this.stats))))
    val result = nonNormalizedValues.mapValues(normalizeEntry).cache()
    result.count()
    result
  }

  /**
    * Normalizes a given list of dimensions.
    * @param entry A list of dimensions to normalize.
    * @return A vector representing normalized values for the input dimensions.
    */
  private def normalizeEntry(entry : Array[Dimension]) = {
    val vectorArray = entry.map(d => d.getNormalizedDims).reduceLeft((a, b) => a ++ b)
    Vectors.dense(vectorArray)
  }

  private var data = initDimensions(dimensions)

  /**
    * Returns all customer IDs which we know about.
    * @return An array of all customer IDs we know about.
    */
  def getAllCustomerIDs : Array[Integer] = data.map(_._1).collect()

  /**
    * Returns the dimensional vector for the given customer ID.
    * @param customerNumber Customer ID to query for.
    * @return A vector representing all of the used dimensions for the given customer.
    */
  def getVector(customerNumber : Integer) : Vector = {
    data.filter{ case (c, _) => c.equals(customerNumber) }.values.take(1).apply(0)
  }

  /**
    * @return Mapping from customer ID to a vector representing all used dimensions.
    */
  def getAllDimensions: RDD[(Integer, Vector)] = data

  /**
    * Set which dimensions should be used when calculating vectors.
    * @param dimensions List of dimensions to use.
    * @return The new mapping of customer ID to dimensional vectors based on the new dimensions.
    */
  def setDimensions(dimensions : Array[String]): RDD[(Integer, Vector)] = {
    if (!dimensions.zip(this.dimensions).forall{ case (x, y) => x == y}) data = initDimensions(dimensions)
    data
  }

  /**
    * Given all information for a single customer, returns an array of dimensions representing that customer.
    * @param info Condensed information needed for a given customer.
    * @param dims The list of dimensions which should be used in calculating the values.
    * @param stats Stats that may have been calculated for all dimensions so far, for use in normalization.
    * @return An array of dimensions for the given customer and requested dimensions.
    */
  private def getLabelledDimensions(info: (Row, InvoiceStats, Double), dims : Array[String], stats : Option[DimensionalStats] = None) : Array[Dimension] = info match {
    case (row, s, avgStyleYear) => dims.map(d => getDimension(d, row, s, avgStyleYear, stats))
  }

  /**
    * Returns the value for the requested dimension for a given customer.
    * @param dim Dimension which is being requested.
    * @param row A row from a data frame which contains all directly queried data necessary.
    * @param invoiceStats Statistics about invoices for the given customer.
    * @param averageStyleYear The average style year which the customer has purchased.
    * @param stats The stats for all dimensions to be used during normalization.
    * @return A dimension object representing the requested dimension for the given customer.
    */
  private def getDimension(dim : String, row : Row, invoiceStats : InvoiceStats, averageStyleYear : Double, stats : Option[DimensionalStats]) : Dimension = dim match {
    case "Simple Life" | "Family Life" | "Elite" | "Active Adult Elite" | "Renters" | "zip" =>  Dimension.apply(dim, App.convertCommaStringToDouble(row.getAs[String](dim)), stats)
    case "channel" | "CREDIT_TYPE" | "SHWONL_FLAG" => val rowValue = row.get(row.fieldIndex(dim)); getEnumBasedDimension(possibleCustomerColumnValues.apply(dim), if (rowValue == null) "" else rowValue.toString, dim, stats)
    case "ACTIVE_DATE" => Dimension.apply(dim, {val date = row.getAs[String](dim); if (date.equals("#N/A")) 1946.0 else date.split("/")(2).toDouble}, stats)
    case "COMMON_CUSTOMER" => Dimension.apply(dim, row.getAs[Integer](dim).doubleValue(), stats)
    case "PROD_DESC" | "CHO_SUB_CAT" | "CPT_HDS_OTH" | "Mix" => Dimension.apply(dim, invoiceStats.getKeysForColumn(dim).map(key => invoiceStats.getColumnPercentage(dim, key)).toArray, stats)
    // DTODO: If we decided to use this metric in the real system, we'd want to evaluate whether to use Style Year,
    // Invoice Year, or Age of Product When Bought. For now, all invoices are from 2016, so Style Year works for now.
    case "StyleYear" => Dimension.apply(dim, averageStyleYear, stats)
    case "PILE_WEIGHT" | "AverageExpenditure" => Dimension.apply(dim, invoiceStats.getColumnAverage(dim.replaceFirst("Average", "")), stats)
    case "TotalExpenditure" => Dimension.apply(dim, invoiceStats.getColumnTotal(dim.replaceFirst("Total", "")), stats)
  }

  /**
    * Returns the dimension object for a customer when the dimension is based on which of a given set of values the customer
    * matches, assuming the customer can only have one value (e.g. channel, CREDIT_TYPE, etc)
    * @param possibleValues All possible values of the column.
    * @param value The value which this customer has.
    * @param dim The dimension name being created.
    * @param stats The stats for all dimensions to be used during normalization.
    * @return A dimension object which contains 0 for all values except for one value which contains a 1, representing the
    *         actual value for the given column for this customer.
    */
  private def getEnumBasedDimension(possibleValues: Array[String], value : String, dim : String, stats : Option[DimensionalStats]) : Dimension = {
    Dimension.apply(dim, possibleValues.indices.map(i => if (possibleValues.apply(i) == value) 1.0 else 0.0).toArray, stats)
  }

  /**
    * @return All necessary information for all customers for determining dimensional information, in RDD form.
    */
  private def getAllDimensionInfo = {
    val percentageInCategoryByCustomer = InvoiceStats.getInvoiceStats(dataSet)

    val averageSellStyleYears = getAverageStyleYearByCustomer

    val filteredHouseholds = dataSet.households.select("Geography Id", "Simple Life", "Family Life", "Elite", "Active Adult Elite", "Renters")
    val filteredCustomers = dataSet.customers.select("zip", "COMMON_CUSTOMER", "CUSTOMER_NO", "channel", "CREDIT_TYPE", "SHWONL_FLAG", "ACTIVE_DATE")
    val customerInfo = filteredHouseholds.join(filteredCustomers, filteredHouseholds.apply("Geography Id").equalTo(filteredCustomers.apply("zip")))
    val prodCategoryAdded = customerInfo.map(r => (r.getAs[Integer]("CUSTOMER_NO"), r)).join(percentageInCategoryByCustomer)
    val result = prodCategoryAdded.join(averageSellStyleYears).mapValues{case ((r, m), y) => (r, m, y)}.cache()
    result.count()
    result
  }

  /**
    * @return An RDD mapping the average bought style year for all customers.
    */
  private def getAverageStyleYearByCustomer = {
    val sellStyleYears = dataSet.invoices.map(r => {
      ((r.getAs[String]("SELLING_STYLE_NO"), r.getAs[Integer]("CUST_REF_NO")), r.getAs[Integer]("CUSTOMER_NO"))
    }).join(dataSet.sellStyles.map(r => ((r.getAs[String]("SELLING_STYLE_NO"), r.getAs[Integer]("CUST_REF_NO")), r.getAs[String]("SELL_INTRO_DATE"))))
    val sellStyleParsedYears = sellStyleYears.map { case (_, (c, d)) => (c, d.split("-").apply(0).toDouble) }
    val averageSellStyleYears = sellStyleParsedYears.combineByKey(y => (y, 1.0),
      (a: (Double, Double), y) => (a._1 + y, a._2 + 1),
      (a: (Double, Double), b: (Double, Double)) => (a._1 + b._1, a._2 + b._2)).mapValues { case (t, c) => t / c }
    averageSellStyleYears
  }
}

object DimensionInfo {
  val allDimensions : Array[String] = Array.apply("Elite", "zip", "Renters", "COMMON_CUSTOMER", "channel", "PROD_DESC", "StyleYear",
    "Simple Life", "Family Life", "Active Adult Elite", "CHO_SUB_CAT", "CPT_HDS_OTH", "PILE_WEIGHT", "CREDIT_TYPE", "SHWONL_FLAG", "AverageExpenditure",
    "TotalExpenditure", "Mix", "ACTIVE_DATE")
}
