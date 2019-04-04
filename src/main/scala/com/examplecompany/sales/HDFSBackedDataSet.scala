package com.examplecompany.sales

import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext}

/**
  * Dataset which retrieves its data from a hardcoded location on HDFS.
  * This is mainly meant for testing without having to access Hive.
  */
class HDFSBackedDataSet(reader : DataFrameReader, sqlContext : SQLContext) extends java.io.Serializable with DataSet {
  private val _directoryBase = "Resources/Data/OriginalData"
  private val _customers = reader.load(_directoryBase + "/customer.csv").filter("CUSTOMER_STATUS_CD != 'D'").cache()
  private val _invoices : DataFrame  = reader.load(_directoryBase + "/orderinv_line.csv").selectExpr("*", "QUANTITY * UNIT_PRICE as Expenditure").where("Expenditure > 0").cache()
  private val _households : DataFrame  = reader.load(_directoryBase + "/homeowner_zip.csv").cache()
  private val _sellStyles : DataFrame  = reader.load(_directoryBase + "/sell_style.csv").filter("SELLING_STYLE_NAME != '\"' AND SELLING_STATUS_CD != 'D'").cache()//initSellStyles(reader, sqlContext, directoryBase).cache()
  private val _mixes : DataFrame = reader.load(_directoryBase + "/MixStyle.csv").cache()

  def customers : DataFrame = _customers
  def invoices : DataFrame  = _invoices
  def households : DataFrame  = _households
  def sellStyles : DataFrame  = _sellStyles
  def mixes : DataFrame = _mixes
}

object HDFSBackedDataSet {
  def apply(sqlContext : SQLContext) : DataSet = {
    val reader = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true")
    new HDFSBackedDataSet(reader, sqlContext)
  }
}
