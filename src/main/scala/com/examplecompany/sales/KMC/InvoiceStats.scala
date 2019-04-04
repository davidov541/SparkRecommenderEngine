package com.examplecompany.sales.KMC

import com.examplecompany.sales.{App, DataSet}
import org.apache.spark.rdd.RDD

/**
  * Calculates the data for any dimensions which depend on information about all
  * invoices the customer has associated with them.
  */
class InvoiceStats extends java.io.Serializable {
  private val allColumnPercentages : collection.mutable.Map[String, Map[Int, Double]] = collection.mutable.Map[String, Map[Int, Double]]()
  private val allColumnAverages : collection.mutable.Map[String, Double] = collection.mutable.Map[String, Double]()
  private val allColumnTotals : collection.mutable.Map[String, Double] = collection.mutable.Map[String, Double]()
  private val maxColumnKeys : collection.mutable.Map[String, Int] = collection.mutable.Map[String, Int]()

  /**
    * Adds a given column to stats which should be calculated.
    * The percentages only will all be calculated.
    * @param columnName Name of the column being added.
    * @param purchases A list of all purchases, as a mapping from column value to a number representing how much that instance counts for.
    * @param lookup A mapping from enumeration value to a key that represents it.
    * @return The same invoice stats instance.
    */
  def addPercentageColumn(columnName : String, purchases : List[(String, Int)], lookup : Map[String, Int]) : InvoiceStats = {
    maxColumnKeys += (columnName -> lookup.values.max)
    val totalPurchases = purchases.reduce((x, y) => ("", x._2 + y._2))._2
    val percentages = purchases.map { case (d, n) => (lookup.apply(d), n.toDouble / totalPurchases.toDouble) }.toMap
    allColumnPercentages += (columnName -> percentages)
    this
  }

  /**
    * Returns a list of keys for the given column that represents a given value.
    * @param columnName Name of the column to query for.
    * @return An iterable of all valid keys for that given column.
    */
  def getKeysForColumn(columnName : String) : Iterable[Int] = 0 to maxColumnKeys.apply(columnName)

  /**
    * Gets the percentage representing the given value (as a key) for the given column.
    * @param columnName Column to query for.
    * @param key The key representing the value of the column to query for.
    * @return The percentage of the time which the given key appeared in the given column for this customer.
    */
  def getColumnPercentage(columnName : String, key : Integer) : Double = allColumnPercentages.apply(columnName).getOrElse(key, 0.0)

  /**
    * Adds a column to store in terms of averages and total values.
    * @param columnName Name of the column.
    * @param average The average value of the column.
    * @param total The total value of the column across all invoices.
    * @return The same invoice stats instance.
    */
  def addColumnStats(columnName : String, average : Double, total : Double) : InvoiceStats = {
    allColumnAverages += (columnName -> average)
    allColumnTotals += (columnName -> total)
    this
  }

  /**
    * Returns the average value of this column for this customer.
    * @param columnName The name of the column to query.
    * @return The average value of the column.
    */
  def getColumnAverage(columnName : String) : Double = allColumnAverages.apply(columnName)

  /**
    * Returns the total value of this column for this customer.
    * @param columnName The name of the column to query.
    * @return The total value of the column.
    */
  def getColumnTotal(columnName : String) : Double = allColumnTotals.apply(columnName)
}

object InvoiceStats {
  /**
    * Gets InvoiceStats objects for every customer in the given data set.
    * @param dataSet Data set to query from.
    * @return An RDD containing a mapping of customer IDs to an invoice stats instance.
    */
  def getInvoiceStats(dataSet : DataSet): RDD[(Integer, InvoiceStats)] = {
    val initial = dataSet.customers.map(r => (r.getAs[Integer]("CUSTOMER_NO"), new InvoiceStats()))
    val withProdDesc = addInvoiceDimensionalColumn(dataSet, "PROD_DESC", initial)
    val withSubCat = addInvoiceDimensionalColumn(dataSet, "CHO_SUB_CAT", withProdDesc)
    val withType = addInvoiceDimensionalColumn(dataSet, "CPT_HDS_OTH", withSubCat)
    val withMix = addMixColumn(dataSet, withType)
    val withPileWeight = addAverageStyleColumn(dataSet, "PILE_WEIGHT", withMix)
    addAverageInvoiceColumn(dataSet, "Expenditure", withPileWeight)
  }

  /**
    * Adds a column to the invoice stats for Mixes bought by the customer.
    * @param dataSet The data set to query from.
    * @param invoiceStats The current set of invoice stats which should be updated.
    * @return An updated mapping of customer IDs to invoice stats, with the Mix column added.
    */
  private def addMixColumn(dataSet: DataSet, invoiceStats: RDD[(Integer, InvoiceStats)]) : RDD[(Integer, InvoiceStats)] = {
    val (lookup, _) = App.createLookup(dataSet.mixes.select("Mix").map(_.getAs[String](0)))

    val allProducts = dataSet.mixes.map(r => ((r.getAs[String]("SellStyleNo"), r.getAs[Integer]("CustomerRefNo")), Array.apply(r.getAs[String]("Mix")))).reduceByKey(_ ++ _)
    val allPurchases = dataSet.invoices.select("CUSTOMER_NO", "CUST_REF_NO", "SELLING_STYLE_NO").map(r => ((r.getAs[String]("SELLING_STYLE_NO"), r.getAs[Integer]("CUST_REF_NO")), r.getAs[Integer]("CUSTOMER_NO")))
    val numPurchasesByCustomerAndDesc = allProducts.join(allPurchases).flatMap { case (_, (d, c)) => d.map(mix => ((c, mix), 1)) }.reduceByKey((x, y) => x + y)
    val numPurchasesByCustomer = numPurchasesByCustomerAndDesc.map { case ((c, d), n) => (c, List.apply((d, n))) }.reduceByKey((x, y) => x ++ y)
    invoiceStats.join(numPurchasesByCustomer).mapValues{ case (s, l) => s.addPercentageColumn("Mix", l, lookup)}
  }

  /**
    * Adds a column to the invoice stats which is enum based, and should be reported as percentages.
    * @param dataSet The data set to query from.
    * @param columnName Name of the column to add.
    * @param invoiceStats The current set of invoice stats which should be updated.
    * @return An updated mapping of customer IDs to invoice stats, with the given column added.
    */
  private def addInvoiceDimensionalColumn(dataSet : DataSet, columnName : String, invoiceStats : RDD[(Integer, InvoiceStats)]) : RDD[(Integer, InvoiceStats)] = {
    val (lookup, _) = App.createLookup(dataSet.sellStyles.select(columnName).map(_.getAs[String](0)))

    val allProducts = dataSet.sellStyles.select("SELLING_STYLE_NO", "CUST_REF_NO", columnName).map(r => ((r.getAs[String]("SELLING_STYLE_NO"), r.getAs[Integer]("CUST_REF_NO")), r.getAs[String](columnName)))
    val allPurchases = dataSet.invoices.select("CUSTOMER_NO", "CUST_REF_NO", "SELLING_STYLE_NO").map(r => ((r.getAs[String]("SELLING_STYLE_NO"), r.getAs[Integer]("CUST_REF_NO")), r.getAs[Integer]("CUSTOMER_NO")))
    val numPurchasesByCustomerAndDesc = allProducts.join(allPurchases).map { case (_, (d, c)) => ((c, d), 1) }.reduceByKey((x, y) => x + y)
    val numPurchasesByCustomer = numPurchasesByCustomerAndDesc.map { case ((c, d), n) => (c, List.apply((d, n))) }.reduceByKey((x, y) => x ++ y)
    invoiceStats.join(numPurchasesByCustomer).mapValues{ case (s, l) => s.addPercentageColumn(columnName, l, lookup)}
  }

  /**
    * Adds a column to the invoice stats which is statistics based, and is gathered from the styles bought by the customer.
    * @param dataSet The data set to query from.
    * @param columnName Name of the column to add.
    * @param invoiceStats The current set of invoice stats which should be updated.
    * @return An updated mapping of customer IDs to invoice stats, with the given column added.
    */
  private def addAverageStyleColumn(dataSet : DataSet, columnName : String, invoiceStats : RDD[(Integer, InvoiceStats)]) : RDD[(Integer, InvoiceStats)] = {
    val allProducts = dataSet.sellStyles.select("SELLING_STYLE_NO", "CUST_REF_NO", columnName).map(r => ((r.getAs[String]("SELLING_STYLE_NO"), r.getAs[Integer]("CUST_REF_NO")), r.getAs[Double](columnName)))
    val allPurchases = dataSet.invoices.select("CUSTOMER_NO", "CUST_REF_NO", "SELLING_STYLE_NO").map(r => ((r.getAs[String]("SELLING_STYLE_NO"), r.getAs[Integer]("CUST_REF_NO")), r.getAs[Integer]("CUSTOMER_NO")))
    val purchasesByCustomerAndCol = allProducts.join(allPurchases).map { case (_, (d, c)) => (c, (d, 1.0)) }.reduceByKey{ case ((d1, c1), (d2, c2)) => (d1 + d2, c1 + c2)}
    invoiceStats.join(purchasesByCustomerAndCol).mapValues{ case (s, (sum, count)) => s.addColumnStats(columnName, sum / count, sum)}
  }

  /**
    * Adds a column to the invoice stats which is statistics based, and is gathered from the invoices created by the customer.
    * @param dataSet The data set to query from.
    * @param columnName Name of the column to add.
    * @param invoiceStats The current set of invoice stats which should be updated.
    * @return An updated mapping of customer IDs to invoice stats, with the given column added.
    */
  private def addAverageInvoiceColumn(dataSet : DataSet, columnName : String, invoiceStats : RDD[(Integer, InvoiceStats)]) : RDD[(Integer, InvoiceStats)] = {
    val allPurchases = dataSet.invoices.select("CUSTOMER_NO", columnName).map(r => (r.getAs[Integer]("CUSTOMER_NO"), (r.getAs[Double](columnName), 1.0)))
    val purchasesByCustomerAndCol = allPurchases.reduceByKey{ case ((d1, c1), (d2, c2)) => (d1 + d2, c1 + c2)}
    invoiceStats.join(purchasesByCustomerAndCol).mapValues{ case (s, (sum, count)) => s.addColumnStats(columnName, sum / count, sum)}
  }
}
