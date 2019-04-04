package com.examplecompany.sales

import org.apache.spark.sql.DataFrame

/**
  * Trait describing a class which contains all of the input information we need.
  */
trait DataSet extends java.io.Serializable {
  /**
    * @return Information on all customers
    */
  def customers : DataFrame

  /**
    * @return Information about all invoices that have been processed.
    */
  def invoices : DataFrame

  /**
    * @return All demographic information, based on zip codes.
    */
  def households : DataFrame

  /**
    * @return Information about all sell styles.
    */
  def sellStyles : DataFrame

  /**
    * @return A mapping from sell styles and customer reference numbers to mixes.
    *         There may be multiple mixes per sell style/customer reference tuple.
    */
  def mixes : DataFrame
}