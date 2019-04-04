package com.examplecompany.sales.MBA

import com.examplecompany.sales.{DataSet, RecommenderSession}
import org.apache.spark.rdd.RDD

/**
  * Created by davidmcginnis on 3/13/17.
  */
class MBASession(private val dataSet : DataSet) extends java.io.Serializable with RecommenderSession {
  private val fullInvoiceInfo = dataSet.invoices.map(r => (r.getAs[Int]("CUSTOMER_NO"), r.getAs[String]("SELLING_STYLE_NO"), r.getAs[Integer]("CUST_REF_NO"))).distinct.cache()
  private val invoicesForJoinByCustomer = fullInvoiceInfo.map { case (customer, item, ref) => (customer, (customer, item, ref)) }.cache()
  private val itemCounts = fullInvoiceInfo.map { case (_, item, ref) => ((item, ref), 1) }.reduceByKey((x, y) => x + y).cache()

  override def getSuggestions(customerID: Integer): RDD[(String, Double)] = {
    // Gets list of purchases by the customer.
    val allItemsBoughtByCustomer = fullInvoiceInfo.filter { case (customer, _, _) => customer == customerID }

    // Gets list of all purchases by someone other than the customer of products the customer has bought.
    val invoicesForJoinByItem = fullInvoiceInfo.filter { case (customer, _, _) => customer != customerID }.map { case (customer, item, ref) => ((item, ref), (customer, item, ref)) }
    val itemsBoughtByCustomerForJoin = allItemsBoughtByCustomer.map { case (customer, item, ref) => ((item, ref), (customer, item, ref)) }
    val sameItemsBoughtByOtherCustomer = invoicesForJoinByItem.join(itemsBoughtByCustomerForJoin).map { case (_, (x, _)) => x }

    // Gets list of all purchases by someone other than the customer (but still has purchased something the customer has purchased).
    val sameItemsByOtherCustomersForJoin = sameItemsBoughtByOtherCustomer.map { case (customer, item, ref) => (customer, (customer, item, ref)) }
    val allItemsBoughtBySimilarCustomers = invoicesForJoinByCustomer.join(sameItemsByOtherCustomersForJoin).map { case (_, (x, _)) => x }.distinct()

    // Compute all pairs of items from the previous query which were bought by the same customer.
    val uncommonItemsForJoin = allItemsBoughtBySimilarCustomers.map { case (customer, item, ref) => (customer, (customer, item, ref)) }
    val itemPairs = sameItemsByOtherCustomersForJoin.join(uncommonItemsForJoin).distinct

    val reducedItemPairs = itemPairs.map { case (_, ((_, item1, ref1), (_, item2, ref2))) => (((item1, ref1), (item2, ref2)), 1) }.reduceByKey((x, y) => x + y)

    // Compute percentage of time an item B was bought by the same customer that bought item A.
    val reducedItemPairsForJoin = reducedItemPairs.map { case (((item1, ref1), (item2, ref2)), count) => ((item1, ref1), (((item1, ref1), (item2, ref2)), count)) }
    reducedItemPairsForJoin.join(itemCounts).map { case (_, (((_, (item2, ref2)), count), totalCount)) => ((item2, ref2), (count.toDouble / totalCount.toDouble, 1.0)) }.
      reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map{ case ((item, ref), (sum, count)) => (item, sum / count)}
  }
}
