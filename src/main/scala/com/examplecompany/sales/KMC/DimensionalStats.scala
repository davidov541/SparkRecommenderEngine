package com.examplecompany.sales.KMC

import org.apache.spark.rdd.RDD

/**
  * Houses the information needed about a given dimension for all customers, in order to allow for normalization
  * of the dimension.
  */
class DimensionalStats(private val means : Array[Dimension],
                       private val stdDeviations : Array[Dimension]) extends java.io.Serializable {

  /**
    * Normalizes the given dimension.
    * @param values The list of values to normalize.
    * @param dimension The name of the dimension being normalized.
    * @return A new set of values which are now normalized.
    */
  def normalizeForDimension(values : Array[Double], dimension : String) : Array[Double] = {
    (values, getMean(dimension), getStdDeviation(dimension)).zipped.map{ case (value, mean, stdDeviation) => applyStats(value, mean, stdDeviation)}
  }

  /**
    * Normalizes the value given the mean and standard deviation values.
    * @param value The value to be normalized.
    * @param mean The mean value for this index.
    * @param stdDeviation The standard deviation value for this index.
    * @return The normalized version of the input value.
    */
  private def applyStats(value : Double, mean : Double, stdDeviation : Double) : Double = {
    if (stdDeviation <= 0) value - mean else (value - mean) / stdDeviation
  }

  /**
    * Determines the mean values for the given dimension.
    * @param dimension The dimension to query for.
    * @return An array of doubles which represent the means for those values.
    */
  private def getMean(dimension : String) = means.find(d => d.getName == dimension).get.getUnscaledDims

  /**
    * Determines the standard deviation values for the given dimension.
    * @param dimension The dimension to query for.
    * @return An array of doubles which represent the standard deviations for those values.
    */
  private def getStdDeviation(dimension : String) = stdDeviations.find(d => d.getName == dimension).get.getUnscaledDims
}

object DimensionalStats {
  /**
    * Creates a DimensionStats object given the list of all dimensions.
    * @param dimensionData A list of all dimensions for all customers.
    * @return A dimensional stats object which contains information to normalize values.
    */
  def apply(dimensionData : RDD[Array[Dimension]]): DimensionalStats = {
    val count = dimensionData.count().toDouble
    val sums = dimensionData.reduce((a, b) => sumStats(a, b))
    val means = sums.map(a => a.divide(count))
    val stdDeviationSums = dimensionData.map(mapStdDeviation(_, means)).reduce((a, b) => sumStats(a, b))
    val stdDeviations = stdDeviationSums.map(a => a.divide(count).sqrt())
    new DimensionalStats(means, stdDeviations)
  }

  /**
    * Sum all values of the two dimensions together.
    * @param one One array of dimensions.
    * @param other Another corresponding array of dimensions.
    * @return A new array of dimensions which represents the sum of the values of the two arrays of dimensions.
    */
  private def sumStats(one : Array[Dimension], other : Array[Dimension]) : Array[Dimension] = {
    one.zip(other).map{ case (on, ot) => on.add(ot)}
  }

  /**
    * Determines the inner portion of the standard deviation equation for a given dimension.
    * The new values will follow the equation (v - m)^^2, where v is the current value,
    * and m is the value in the mean dimension.
    * @param data Data to apply the standard deviation based equation to.
    * @param means The mean values for the values.
    * @return A new dimension with the values applied to the equation above.
    */
  private def mapStdDeviation(data : Array[Dimension], means : Array[Dimension]) = {
    data.zip(means).map{ case (v, m) => v.applyStdDeviation(m)}
  }
}