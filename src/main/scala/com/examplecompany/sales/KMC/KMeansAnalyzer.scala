package com.examplecompany.sales.KMC

import com.examplecompany.sales.{ParameterAnalyzer, DataSet, Scorer}

/**
  * Main analyzer for the K-Means Clustering algorithm. Handles scoring for hyperparameters and dimensional analysis,
  * as well as simple accuracy check.
  */
class KMeansAnalyzer(dataSet : DataSet, private val numModelsToTry : Integer, seed : Long, suggester : KMeansSuggester) extends ParameterAnalyzer {
  /**
    * Runs an analysis of different hyperparameter combinations, returning a string which reports the scores for different combinations.
    * @param scorer The scorer to use when determining the result of the analysis.
    * @return A string report that can be shown to the user about the effectiveness of different sets of hyperparameters.
    */
  def runHyperParameterAnalysis(scorer : Scorer) : String = {
    print(startProgressBar("Evaluating KMC Hyper-parameters"))

    val parameterSets = getParameterSets

    val header = f"${"Score"}%-10s" + "\t" + f"${"Clusters"}%-10s" + "\t" + f"${"Iterations"}%-10s" + "\n"
    val body = parameterSets.zipWithIndex.map{ case ((numIterations, numClusters), currentCombination) =>
      val scoreString = (1 to numModelsToTry).map(_ =>
        scorer.getFullScoreForCluster(suggester.createSession(numClusters = numClusters, numIterations = numIterations, seed = Some.apply(seed)), isMoney = true)).max

      print(updateProgressBar(currentCombination + 1, currentCombination, parameterSets.length))

      f"$scoreString%-10s" + f"\t$numClusters%-10d" + f"\t$numIterations%-10d"
    }.mkString("\n")
    print(finishProgressBar())
    header + body
  }

  /**
    * @return A list of hyperparameter sets to try.
    */
  private def getParameterSets : Array[(Int, Int)] = {
    val numIterationsOptions = Array.apply(1, 2, 5, 7, 9, 10, 15, 20, 25)
    val numClustersOptions = 10 to 200 by 10
    for {numIterations <- numIterationsOptions; numClusters <- numClustersOptions} yield (numIterations, numClusters)
  }

  /**
    * Runs an analysis of different dimension combinations, returning a string which reports the scores for different combinations.
    * @param scorer The scorer to use when determining the result of the analysis.
    * @return A string report that can be shown to the user about the effectiveness of different sets of dimensions.
    */
  def runDimensionalAnalysis(scorer : Scorer, rank : Integer) : String = {
    val dimensions = Array.apply("zip", "StyleYear", "Active Adult Elite", "ACTIVE_DATE", "CHO_SUB_CAT",
      "Mix", "AverageExpenditure", "CPT_HDS_OTH", "PROD_DESC", "Elite", "Simple Life", "Family Life", "COMMON_CUSTOMER")

    def choose(n : Integer, k : Integer): Integer = { if (k == 0 || k == n) 1 else choose(n - 1, k - 1) + choose(n - 1, k) }

    val totalNumberOfCombinations = choose(dimensions.length, rank).doubleValue()

    print(startProgressBar("Evaluating KMC Dimensions for rank " + rank.toString))

    val output = f"${"Score"}%-10s" + "\t" + dimensions.map(d => f"$d%-20s").mkString("\t") + "\n" +
      dimensions.combinations(rank).toArray.zipWithIndex.map{ case (currentDimensions, currentCombination) =>
        val scoreString = (1 to numModelsToTry).map(_ => scorer.getFullScoreForCluster(suggester.createSession(currentDimensions, seed = Some.apply(seed)), isMoney = true)).max

        print(updateProgressBar(currentCombination + 1, currentCombination, totalNumberOfCombinations))

        f"$scoreString%-10s" + dimensions.map(d => if (currentDimensions.contains(d)) d else "").map(d => f"\t$d%-20s").reduceLeft((a, b) => a + b) + "\n"
      }.reduce((a, b) => a + b)
    print(finishProgressBar())
    output
  }

  /**
    * Runs an accuracy check of the current default parameters and dimensions for a K-Means Clustering recommendation.
    * @param scorer The scorer to use when determining the result of the analysis.
    * @return A string report that can be shown to the user about the current accuracy of the algorithm.
    */
  def checkAccuracy(scorer : Scorer) : String = {
    val header = "Evaluating KMC Accuracy\n" + f"${"Score"}%-10s" + "\n"
    val scoreString = scorer.getFullScoreForCluster(suggester.createSession(KMeansSuggester.getBestDimensions), isMoney = true)
    val body = f"$scoreString%-10s"
    header + body
  }
}
