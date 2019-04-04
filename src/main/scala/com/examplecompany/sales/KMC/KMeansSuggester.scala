package com.examplecompany.sales.KMC

import com.examplecompany.sales.DataSet
import org.apache.spark.mllib.clustering.KMeans

/**
  * Top-level class for the K-Means Clustering algorithm.
  * Handles training models and creating sessions for recommendations based on the data set passed in.
  */
class KMeansSuggester(private val dataSet : DataSet) {
  private var dimensionInfo : Option[DimensionInfo] = None

  /**
    * Trains a new model and creates a KMeansSession object to wrap that model.
    * If no parameters are supplied, the best known parameters are used.
    * @param newDimensions Dimensions to use when training.
    * @param numClusters The number of clusters to create of customers.
    * @param numIterations The number of iterations to go through in the training model.
    * @param seed The seed to use. This should be used if you want to ensure getting the same model
    *             multiple times without saving and loading the model.
    * @return A KMeansSession instance which represents the model created.
    */
  def createSession(newDimensions : Array[String] = KMeansSuggester.getBestDimensions,
                    numClusters : Integer = 100, numIterations : Integer = 7, seed : Option[Long] = None) : KMeansSession = {
    val localDimensionInfo = dimensionInfo match {
      case None => val tempDimInfo = new DimensionInfo(dataSet, newDimensions); dimensionInfo = Some.apply(tempDimInfo); tempDimInfo
      case Some(d) => d
    }
    localDimensionInfo.setDimensions(newDimensions)

    val normalizedVectors = localDimensionInfo.getAllDimensions.values.cache()

    val model = if (seed.isDefined) KMeans.train(normalizedVectors, numClusters, numIterations, 1, KMeans.K_MEANS_PARALLEL, seed.get) else KMeans.train(normalizedVectors, numClusters, numIterations)

    normalizedVectors.unpersist(blocking = false)

    new KMeansSession(model, localDimensionInfo, dataSet)
  }
}

object KMeansSuggester {
  /**
    * @return An array of strings representing the dimensions which give us the best results currently.
    */
  def getBestDimensions : Array[String] = Array.apply("StyleYear", "AverageExpenditure", "CPT_HDS_OTH")
}