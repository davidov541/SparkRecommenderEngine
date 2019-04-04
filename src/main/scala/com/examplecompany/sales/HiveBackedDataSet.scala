package com.examplecompany.sales

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, DataFrameReader}

/**
  * DataSet which gets its data from a Hive table colocated with the Hadoop cluster this job is running on.
  */
class HiveBackedDataSet(reader : DataFrameReader, sqlContext : HiveContext) extends DataSet {
  private val directoryBase = "/user/raj_ops/Recommender/OriginalData"
  private val _customersSQL = """select customer_no, customer_status_cd, bill_to_name, common_customer, store_type_cd,
                                 group_acct_no, credit_line, channel, phy_addr_line1, phy_addr_line2, phy_addr_city,
                                 phy_addr_state,PHY_ADDR_POSTAL_CD, credit_type, shwonl_flag, active_date
                                 from sdwp.CUSTOMER
                                 where customer_status_cd in ('A','Z')"""
  private val _customers = sqlContext.sql(_customersSQL).cache()
  private val _invoicesSQL = """select division_id,invoice_date,customer_no,selling_style_no,cust_ref_no,inv_style_no,
                                credited_sell_co,quantity,discount_cd,unit_price,(net_amt-inv_fob_cost)/net_amt as marginpct,
                                QUANTITY * UNIT_PRICE as Expenditure
                                from sdwp.orderinv_line a join
                                sdwp.sell_company b on a.credited_sell_co = b.sell_co
                                where invoice_month > '201512'
                                and net_amt <> 0
                                and rcs_cd <> 'S'
                                Expenditure > 0
                                AND division_id in ('02','06','32','34','58','60','63','93','94','96')
                                and orderinv_line_type = '1'"""
  private val _invoices : DataFrame  = sqlContext.sql(_invoicesSQL).cache()
  private val _sellStylesSQL = """select  a.SELLING_STYLE_NO, a.cust_ref_no, a.SELLING_STYLE_NAME, a.SELL_PROD_CD, a.SELLING_BRAND_CD,
                                  COLLECTION_CD, SELL_INTRO_DATE, f.PROD_CD_GROUP, f.PROD_DESC, f.CPT_HDS_OTH, f.CHO_SUB_CAT, d.BRAND_DESC,
                                  d.SELLING_BRAND_GRP, e.PROD_CAT, PILE_WEIGHT, SELLING_STATUS_CD, a.INV_STYLE_NO
                                  from sdwp.SELL_STYLE a, sdwp.SELL_COMPANY b, sdwp.sell_brand d, sdwp.INV_STYLE e, sdwp.PRODUCT_CODE f
                                  where b.SELL_CO = a.SELL_STYLE_SELL_CO
                                  and f.PROD_CD=a.SELL_PROD_CD
                                  and a.selling_brand_cd = d.selling_brand_cd
                                  and a.inv_style_no = e.inv_style_no
                                  and a.SELLING_STYLE_NAME != '\"'
                                  and SELLING_STATUS_CD != 'D'"""
  private val _sellStyles : DataFrame  = sqlContext.sql(_sellStylesSQL).cache()
  private val _mixes : DataFrame = reader.load(directoryBase + "/MixStyle.csv").cache()

  def customers : DataFrame = _customers
  def invoices : DataFrame  = _invoices
  def households : DataFrame  = {
    // Currently households is not needed, so we are not going to bother implementing it.
    // If we tweak the dimensions to use households, we can determine how best to load these in production.
    throw new NotImplementedError()
  }
  def sellStyles : DataFrame  = _sellStyles
  def mixes : DataFrame = _mixes
}

object HiveBackedDataSet {
  def apply(hiveContext : HiveContext) : DataSet = {
    val reader = hiveContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true")
    new HiveBackedDataSet(reader, hiveContext)
  }
}