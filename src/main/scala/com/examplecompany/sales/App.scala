package com.examplecompany.sales

import com.examplecompany.sales.CF._
import com.examplecompany.sales.GraphFrames.{GraphFramesAccuracyAnalyzer, GraphFramesSession, GraphFramesSuggester}
import com.examplecompany.sales.KMC._
import com.examplecompany.sales.MBA._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.Map
import scala.util.Random

/**
 * Main class for the program.
 */
class App(sparkConf : SparkConf, testMode : Boolean, private val seed : Long, mixesToExclude : Array[String],
          expirationMonths : Integer, export : Boolean) {
  private val conf = sparkConf
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
    registerKryoClasses(Array.apply(classOf[App], classOf[CFHyperParameterAnalyzer], classOf[CFSession], classOf[CFSuggester],
      classOf[DimensionInfo], classOf[KMeansAnalyzer], classOf[KMeansSuggester], classOf[KMeansSession], classOf[ParameterAnalyzer],
      classOf[MBAAccuracyAnalyzer], classOf[MBASession], classOf[MBASuggester], classOf[RecommenderSession], classOf[InvoiceStats],
      classOf[DimensionalStats], classOf[HDFSBackedDataSet], classOf[HiveBackedDataSet], classOf[Dimension]))

  private val sc = new SparkContext(conf)
  if (testMode) sc.setLogLevel("WARN")
  private val sqlContext = new SQLContext(sc)
  private val hiveContext = new HiveContext(sc)

  private val rawDataSet = if (testMode) HDFSBackedDataSet.apply(sqlContext) else HiveBackedDataSet.apply(hiveContext)

  private var scorer : Option[Scorer] = None
  private val mbaSuggester = new MBASuggester()
  private val kMeansSuggester = new KMeansSuggester(rawDataSet)
  private val cfSuggester = CFSuggester.apply(rawDataSet)
  private val graphFramesSuggester = new GraphFramesSuggester(rawDataSet)
  private val mbaAnalyzer = new MBAAccuracyAnalyzer(rawDataSet, seed, mbaSuggester)
  private val kMeansAnalyzer = new KMeansAnalyzer(rawDataSet, numModelsToTry = 1, seed, kMeansSuggester)
  private val graphFramesAnalyzer = new GraphFramesAccuracyAnalyzer(rawDataSet, seed, graphFramesSuggester)
  private val collaborativeFilteringAnalyzer = new CFHyperParameterAnalyzer(rawDataSet, seed, cfSuggester)

  private val recommendationOutput = if (export) new HiveRecommendationOutput(rawDataSet, hiveContext, expirationMonths, mixesToExclude) else
    new CommandLineRecommendationOutput(rawDataSet, expirationMonths, mixesToExclude)

  private def getScorer = if (scorer.isDefined) scorer.get else { scorer = Some.apply(new Scorer(rawDataSet)); scorer.get}

  def displayMBASuggestions(customerID: Integer): Unit = {
    val suggester = mbaSuggester
    val session = suggester.createSession(rawDataSet)
    recommendationOutput.setTechnique("Market Basket Analysis")
    recommendationOutput.reportSuggestions(session.getSuggestions(customerID), customerID, isMoney = false)
  }

  def displayMBAAccuracy() : String = {
    mbaAnalyzer.runAnalysis(getScorer)
  }

  private var graphFramesSession : Option[GraphFramesSession] = None

  def displayGraphFramesSuggestions(customerID: Integer, saveModelLocation : String, loadModelLocation : String) : Unit = {
    val session: GraphFramesSession = getGraphFramesSession(loadModelLocation)
    graphFramesSession = Some.apply(session)
    val suggestions = session.getSuggestions(customerID)
    recommendationOutput.setTechnique("Graph Frames Modelling")
    recommendationOutput.reportSuggestions(suggestions, customerID, isMoney = true)
  }

  private def getGraphFramesSession(loadModelLocation: String) = {
    val session = if (graphFramesSession.isDefined) graphFramesSession.get else {
      val suggester = graphFramesSuggester
      suggester.createSession(rawDataSet)
    }
    session
  }

  def displayGraphFramesAccuracy() : String = {
    graphFramesAnalyzer.runAnalysis(getScorer)
  }

  private var cfSession : Option[CFSession] = None

  def displayCFSuggestions(customerID: Integer, saveModelLocation : String, loadModelLocation : String) : Unit = {
    val session = if (cfSession.isDefined) cfSession.get else
    if (loadModelLocation.isEmpty) {
      val suggester = cfSuggester
      suggester.createSession(seed = Some.apply(seed))
    } else {
      CFSession.apply(sc, loadModelLocation, rawDataSet)
    }
    cfSession = Some.apply(session)
    val suggestions = session.getSuggestions(customerID)
    if (!saveModelLocation.isEmpty) session.saveModel(sc, saveModelLocation)
    recommendationOutput.setTechnique("Collaborative Filtering")
    recommendationOutput.reportSuggestions(suggestions, customerID, isMoney = false)
  }

  def displayCFAccuracy() : String = {
    collaborativeFilteringAnalyzer.runAccuracyCheck(getScorer)
  }

  def displayCFHyperParameterErrors() : String = {
    collaborativeFilteringAnalyzer.runAnalysis(getScorer)
  }

  private var kmcSession : Option[KMeansSession] = None

  def displayKMCSuggestions(customerID: Integer, saveModelLocation : String, loadModelLocation : String) : Unit = {
    val session: KMeansSession = getKMCSession(loadModelLocation)
    kmcSession = Some.apply(session)
    val suggestions = session.getSuggestions(customerID)
    if (!saveModelLocation.isEmpty) session.saveModel(sc, saveModelLocation)
    recommendationOutput.setTechnique("K-Means Clustering")
    recommendationOutput.reportSuggestions(suggestions, customerID, isMoney = true)
  }

  private def getKMCSession(loadModelLocation: String) = {
    val session = if (kmcSession.isDefined) kmcSession.get else if (loadModelLocation.isEmpty) {
      val suggester = kMeansSuggester
      suggester.createSession(seed = Some.apply(seed))
    } else {
      KMeansSession.apply(sc, loadModelLocation, rawDataSet)
    }
    session
  }

  def displayKMCDimensionalErrorsForRank(rank : Integer) : String = {
    kMeansAnalyzer.runDimensionalAnalysis(getScorer, rank)
  }

  def displayKMCHyperParameterErrors() : String = {
    kMeansAnalyzer.runHyperParameterAnalysis(getScorer)
  }

  def displayKMCAccuracy() : String = {
    kMeansAnalyzer.checkAccuracy(getScorer)
  }

  def displaySeed() : String = {
    "Seed: " + seed.toString
  }

  def displayTopPurchasesForCustomer(customerID : Integer) : String = {
    val customerPurchases = rawDataSet.invoices.filter("CUSTOMER_NO = " + customerID.toString).
      map(r => ((r.getAs[String]("SELLING_STYLE_NO"), r.getAs[Integer]("CUST_REF_NO")), r.getAs[Double]("Expenditure")))
    val sellStyleInfo = rawDataSet.sellStyles.map(r => ((r.getAs[String]("SELLING_STYLE_NO"), r.getAs[Integer]("CUST_REF_NO")), (r.getAs[String]("SELLING_STYLE_NAME"),
      r.getAs[String]("PROD_DESC"), r.getAs[String]("CHO_SUB_CAT"), r.getAs[String]("CPT_HDS_OTH"))))
    val mixes = rawDataSet.mixes.map(r => ((r.getAs[String]("SellStyleNo"), r.getAs[Integer]("CustomerRefNo")), Array.apply(r.getAs[String]("Mix"))))
    val combinedMixes = mixes.reduceByKey((a, b) => a ++ b)
    val sellStyleTotal = sellStyleInfo.join(combinedMixes)
    val topPurchases = customerPurchases.reduceByKey((x, y) => x + y).map(_.swap).sortByKey(ascending = false).take(20).map(_.swap)
    val fullPurchaseInfo = sellStyleTotal.filter{ case ((i, c), _) => topPurchases.map(_._1).contains((i,c))}.map{ case ((i, _), ((n, d, t, s), m)) => (i, n, d, t, s, m.mkString(","))}.take(20)
    val customerName = rawDataSet.customers.filter(f"CUSTOMER_NO = $customerID").select("BILL_TO_NAME").take(1).apply(0).apply(0).toString
    var result = "Top Previously Bought Items for " + customerName + "\n"
    result += "----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n"
    for (suggestion <- fullPurchaseInfo) {
      result += f"${suggestion._1.toString}%-7s" + "\t" + f"${suggestion._2}%-25s" + "\t" + f"${suggestion._3}%-30s" + "\t" +
        f"${suggestion._4}%-30s" + "\t" + f"${suggestion._5}%-20s" + "\t" + f"${suggestion._6}%-20s" + "\n"
    }
    result
  }

  def exportRecommendations(databaseName : String, tableName : String, saveModelLocation : String, loadModelLocation : String) : String = {
    val session = kMeansSuggester.createSession()
    val customerIDs = session.getAllCustomerIDs.take(10)
    hiveContext.sql("USE " + databaseName)
    recommendationOutput.asInstanceOf[HiveRecommendationOutput].setTableName(tableName)
    customerIDs.foreach(c => exportSingleCustomer(session, c))
    "Success!"
  }

  private def exportSingleCustomer(session: KMeansSession, singleCustomerID: Integer) = {
    val prediction = session.getSuggestions(singleCustomerID)
    recommendationOutput.reportSuggestions(prediction, singleCustomerID, isMoney=true)
  }
}

object App {
  def convertCommaStringToDouble(strValue : String) : Double = strValue.replace(",", "").toDouble

  def createLookup(values: RDD[String]): (Map[String, Int], Map[Int, String]) = {
    val lookup = values.distinct.zipWithIndex().map(x => (x._1, x._2.toInt)).collectAsMap()
    val reverseLookup = lookup.map(_.swap)
    (lookup.toMap, reverseLookup.toMap)
  }

  private def displayHelp() : String = {
    "Usage: java recommender.jar [OPTIONS]\n" +
    "\n" +
    "Core Selling Style Recommendation Engine for Shaw Customers\n" +
    "\n" +
    "Options:\n" +
    displayOptionInfo("--exportRecommendations string", "Exports recommendations for all customers to a Hive table. The parameter is the table name for the data to be exported to. The name should be in the form <database>.<table>") +
    displayOptionInfo("--customerNumber integer", "Gets suggestions for the indicated customer, printing the top 20 results to standard out.") +
    displayOptionInfo("--showBoughtItems", "Displays the top purchased items by each customer indicated by --customerNumber. Results are printed to standard out.") +
    displayOptionInfo("--excludeMix string", "Removes any items which are associated with the given mix from suggestions. These items will still be used for training.") +
    displayOptionInfo("--excludeTimeInMonths integer", "If indicated, items bought by the customer within the given number of months from the current date will not be included in suggestions. Otherwise, any purchased items by the customer will not be included.") +
    displayOptionInfo("--saveModel string", "Saves the created model to the location indicated locally, to be used in future runs.") +
    displayOptionInfo("--loadModel string", "Loads the created model from the location indicated locally, using it for any model runs.")
  }

  private def displayOptionInfo(optionName : String, optionDescription : String) = {
    "   " + f"$optionName%-35s" + optionDescription + "\n"
  }

  def main(args : Array[String]) {
    if (args.contains("-h") || args.contains("--help")) { println(displayHelp()); return }

    val testMode = args.contains("--pullFromHDFS")

    val conf = new SparkConf().setAppName("Sales Recommender")
    if (testMode) conf.setMaster("local[*]")

    val expirationMonths = if (args.contains("--excludeTimeInMonths")) args.apply(args.indexOf("--excludeTimeInMonths") + 1).toInt else -1

    val seed = if (args.contains("--seed")) args.apply(args.indexOf("--seed") + 1).toLong else Random.nextLong
    val mixesToExclude = args.indices.filter(i => args.apply(i) == "--excludeMix").map(i => args.apply(i + 1)).toArray
    val export = args.contains("--exportRecommendations")
    val app = new App(conf, testMode, seed, mixesToExclude, expirationMonths, export)

    val defaultCustomerID = 9100058 // World Depot Corporation. Should get back recommendations for hardwood/stone/sealant, no carpet.
    val customerIDs = args.indices.filter(i => args.apply(i) == "--customerNumber").map(i => args.apply(i + 1).toInt)

    val modelSaveLocation = if (args.contains("--saveModel")) args.apply(args.indexOf("--saveModel") + 1) else ""
    val modelLoadLocation = if (args.contains("--loadModel")) args.apply(args.indexOf("--loadModel") + 1) else ""

    if (args.contains("--displaySeed")) println(app.displaySeed())

    if (args.contains("--exportRecommendations")) {
      val pieces = args.apply(args.indexOf("--exportRecommendations") + 1).split('.')
      val databaseName = if (pieces.length == 1) "default" else pieces.apply(0)
      val tableName = if (pieces.length == 1) pieces.apply(0) else pieces.apply(1)
      println(app.exportRecommendations(databaseName, tableName, modelSaveLocation, modelLoadLocation))
    }

    for (customerID <- if (customerIDs.nonEmpty) customerIDs else List.apply(defaultCustomerID)) {
      if (args.contains("--showBoughtItems")) println(app.displayTopPurchasesForCustomer(customerID))
      if (args.contains("--mba")) app.displayMBASuggestions(customerID)
      if (args.contains("--cf")) app.displayCFSuggestions(customerID, modelSaveLocation, modelLoadLocation)
      if (args.contains("--kmc")) app.displayKMCSuggestions(customerID, modelSaveLocation, modelLoadLocation)
      if (args.contains("--graphframes")) app.displayGraphFramesSuggestions(customerID, modelSaveLocation, modelLoadLocation)
    }
    if (args.contains("--mbaAccuracy")) println(app.displayMBAAccuracy())
    if (args.contains("--cfHyper")) println(app.displayCFHyperParameterErrors())
    if (args.contains("--cfAccuracy")) println(app.displayCFAccuracy())
    if (args.contains("--kmcHyper")) println(app.displayKMCHyperParameterErrors())
    if (args.contains("--kmcAccuracy")) println(app.displayKMCAccuracy())
    if (args.contains("--graphframesAccuracy")) println(app.displayGraphFramesAccuracy())
    for (arg <- args.indices) {
      if (args.apply(arg) == "--kmcDim" && arg + 1 < args.length) {
        println(app.displayKMCDimensionalErrorsForRank(args.apply(arg + 1).toInt))
      }
    }
  }
}
