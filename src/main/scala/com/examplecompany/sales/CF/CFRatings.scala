package com.examplecompany.sales.CF

import com.examplecompany.sales.{App, DataSet}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Created by davidmcginnis on 3/29/17.
  */
class CFRatings(dataSet: DataSet, fullTable : DataFrame) {
  private val (sellingStyleLookup, _) = App.createLookup(dataSet.invoices.select("SELLING_STYLE_NO").map(_.getAs[String](0)))
  private val (unscaledRatings, _) = initRatings(dataSet, sellingStyleLookup)

  private def initRatings(dataSet: DataSet, sellingStyleLookup : Map[String, Int]): (RDD[Rating], Double) = {
    val unscaledRatings = fullTable.map { r =>
      val product = sellingStyleLookup.apply(r.getAs[String]("SELLING_STYLE_NO"))
      val user = r.getAs[Int]("CUSTOMER_NO")
      val rating = r.getAs[Double]("QUANTITY") * r.getAs[Double]("UNIT_PRICE")
      Rating.apply(user, product, rating)
    }.cache()

    val maxRating = unscaledRatings.map{ case Rating(_, _, r) => r}.max()

    (unscaledRatings, maxRating)
  }

  def unscaled : RDD[Rating] = unscaledRatings
  // DTODO: Scaling the ratings caused most values to be 0, so we need to determine how to scale without distorting and without having all of the values in one bucket.
  def scaled : RDD[Rating] = unscaledRatings
}
