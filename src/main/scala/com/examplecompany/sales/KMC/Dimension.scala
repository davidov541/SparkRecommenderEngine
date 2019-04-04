package com.examplecompany.sales.KMC

/**
  * Represents the values for a given dimension for a given customer.
  */
class Dimension(private val name : String, private val values : Array[Double], private val dimensionalStats: Option[DimensionalStats] = None) extends java.io.Serializable {
  /**
    * @return The normalized values for this dimension.
    */
  def getNormalizedDims : Array[Double] = dimensionalStats.get.normalizeForDimension(values, name)

  /**
    * @return The unnormalized values for this dimension.
    */
  def getUnscaledDims : Array[Double] = values

  /**
    * @return The user-friendly name of this dimension.
    */
  def getName : String = name

  /**
    * Adds the values of this dimension to another dimension, creating a third dimension.
    * @param other Other dimension to add.
    * @return A new dimension with values that are the sum of the two dimensions.
    */
  def add(other : Dimension) : Dimension = {
    val thisData = values
    val otherData = other.values
    val data = thisData.zip(otherData).map{ case (on, ot) => on + ot}
    new Dimension(name, data, dimensionalStats)
  }

  /**
    * Divides the values of this dimension with another dimension, creating a second dimension.
    * @param scalar A constant value to divide all values by.
    * @return A new dimension with the scaled values.
    */
  def divide(scalar : Double) : Dimension = {
    val data = values.map(_ / scalar)
    new Dimension(name, data, dimensionalStats)
  }

  /**
    * Takes the square root of all values in the dimension, creating a second dimension.
    * @return A new dimension with the square rooted values.
    */
  def sqrt() : Dimension = {
    val data = values.map(Math.sqrt)
    new Dimension(name, data, dimensionalStats)
  }

  /**
    * Determines the inner portion of the standard deviation formula.
    * The resultant dimension has values that are (v - m)^^2, where v is the current value,
    * and m is the value in the mean dimension.
    * @param mean A dimension representing the mean value for this dimension.
    * @return A new dimension representing the calculated values.
    */
  def applyStdDeviation(mean : Dimension) : Dimension = {
    val data = values.zip(mean.values).map{ case (v, m) => Math.pow(v - m, 2)}
    new Dimension(name, data, dimensionalStats)
  }
}

object Dimension {
  /**
    * Creates a dimension of a single value.
    * @param name Name of the dimension
    * @param value Value of the lone value in the dimension.
    * @param dimensionalStats The dimensional stats which control normalization for this dimension.
    * @return A new dimension matching the given parameters.
    */
  def apply(name : String, value : Double, dimensionalStats : Option[DimensionalStats]) : Dimension = {
    new Dimension(name, Array.apply(value), dimensionalStats)
  }

  /**
    * Creates a dimension of multiple values.
    * @param name Name of the dimension
    * @param values An array of values which the dimension will represent.
    * @param dimensionalStats The dimensional stats which control normalization for this dimension.
    * @return A new dimension matching the given parameters.
    */
  def apply(name : String, values : Array[Double], dimensionalStats : Option[DimensionalStats]) : Dimension = {
    new Dimension(name, values, dimensionalStats)
  }
}