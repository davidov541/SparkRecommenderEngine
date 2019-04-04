package com.examplecompany.sales.KMC

import com.examplecompany.sales.{DataSet, RecommenderSession}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Map

/**
  * Session class for K-Means Clustering algorithm.
  */
class KMeansSession(private val model : KMeansModel, private val dimensionInfo : DimensionInfo, dataSet : DataSet) extends java.io.Serializable with RecommenderSession {
  private val invoiceData = dataSet.invoices.
    map(r => (r.getAs[Integer]("CUSTOMER_NO"), (r.getAs[String]("SELLING_STYLE_NO"), r.getAs[Double]("Expenditure")))).cache()

  private val clusterMapping = dimensionInfo.getAllDimensions.mapValues(x => model.predict(x)).cache()
  clusterMapping.count()
  private val clusterPopulations = clusterMapping.map(_.swap).countByKey().toMap

  /**
    * @return A list of all customer IDs which the session can act on.
    */
  def getAllCustomerIDs : Array[Integer] = dimensionInfo.getAllCustomerIDs

  /**
    * Gets all recommendations for the given customer ID.
    * @param customerID ID for the customer to be queried.
    * @return An RDD of tuples of selling style number and score. The scores aren't necessarily sorted.
    *         Highest score means the best suggestion.
    */
  def getSuggestions(customerID: Integer) : RDD[(String, Double)] = {
    val customerVector = dimensionInfo.getVector(customerID)
    val customerCluster = getCustomerClusterForVector(customerVector, clusterPopulations)
    val similarCustomers = clusterMapping.filter { case (customer, cluster) => cluster == customerCluster && customer != customerID }.keys

    invoiceData.join(similarCustomers.map(c => (c, ""))).map { case (_, ((i, n), _)) => (i, n) }.reduceByKey((x, y) => x + y).cache()
  }

  /**
    * Saves the given model off to a file for future use.
    * @param sc Context to use when saving.
    * @param saveLocation The location on HDFS to save the model.
    */
  def saveModel(sc : SparkContext, saveLocation : String) { model.save(sc, saveLocation) }

  /**
    * Determines the cluster which the given customer should correspond to.
    * This handles the situation where a cluster does not have enough customers in it to be useful, by looking for the next
    * closest cluster, until a suitable cluster (more than 5 residents) is found.
    * @param customerVector Vector describing the location of the customer in the space.
    * @param clusterPopulations A map of cluster numbers to how many customers are in each cluster.
    * @param indexToCheck How close of a cluster to check.
    *                     0 will check the cluster the customer belongs to, 1 will check the next closest cluster, etc.
    * @return The cluster index which this customer should belong to for analysis.
    */
  @scala.annotation.tailrec
  private def getCustomerClusterForVector(customerVector : Vector, clusterPopulations : Map[Int, Long], indexToCheck : Integer = 0) : Integer = {
    if (indexToCheck > model.k) return -1
    val closestClusters = model.clusterCenters.sortBy(v => Vectors.sqdist(v, customerVector))
    val closestCluster = closestClusters.apply(indexToCheck)
    val closestClusterNumber = model.clusterCenters.indexOf(closestCluster)
    if (clusterPopulations.apply(closestClusterNumber) > 5) closestClusterNumber else getCustomerClusterForVector(customerVector, clusterPopulations, indexToCheck + 1)
  }
}

object KMeansSession {
  /**
    * Creates a session based on the model saved at the given location.
    * @param sc Context to use to load the model.
    * @param modelLocation The location in HDFS to load the model from.
    * @param dataSet The data set to use for querying.
    * @return A KMeansSession instance that matches the model which was saved.
    */
  def apply(sc : SparkContext, modelLocation : String, dataSet : DataSet): KMeansSession = {
    val model = KMeansModel.load(sc, modelLocation)
    val dimensionInfo = new DimensionInfo(dataSet, KMeansSuggester.getBestDimensions)
    new KMeansSession(model, dimensionInfo, dataSet)
  }
}
