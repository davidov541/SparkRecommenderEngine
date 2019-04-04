package com.examplecompany.sales.CF

import com.examplecompany.sales.{ParameterAnalyzer, DataSet, Scorer}

class CFHyperParameterAnalyzer(dataSet : DataSet, seed : Long, suggester: CFSuggester) extends ParameterAnalyzer {

  def runAnalysis(scorer : Scorer) : String = {
    print(startProgressBar("Evaluating CF Hyper-parameters"))

    val parameterSets = getParameterSets

    val header = f"${"Rank"}%-10s" + "\t" + f"${"Num Iterations"}%-20s" + "\t" + f"${"Lambda"}%-10s" + "\t" +
      f"${"Alpha"}%-20s" + "\t" + f"${"Max Rating"}%-20s" + "\t" + f"${"Use Implicit"}%-20s" + "\t" + f"${"Score"}%-10s" + "\n"

    val body = parameterSets.zipWithIndex.map{ case ((rank, numIterations, lambda, param, useImplicit), currentCombination) =>
      val session = if (useImplicit) suggester.createImplicitSession(rank, numIterations, lambda, param, Option.apply(seed)) else
          suggester.createSession(rank, numIterations, lambda, Option.apply(seed))
      val scoreString = scorer.getFullScoreForCluster(session, isMoney = false)

      print(updateProgressBar(currentCombination + 1, currentCombination, parameterSets.length))

      val alphaString = if (useImplicit) param.toString else "--"
      val maxRatingString = if (useImplicit) "--" else param.toString

      f"$rank%-10d" + "\t" + f"$numIterations%-20d" + "\t" + f"$lambda%-10f" + "\t" +
        f"$alphaString%-20s" + "\t" + f"$maxRatingString%-20s" + "\t" + f"$useImplicit%-10s" + "\t" + f"$scoreString%-10s"
    }.mkString("\n")
    print(finishProgressBar())
    header + body
  }

  def runAccuracyCheck(scorer : Scorer) : String = {
    val header = "Evaluating CF Accuracy\n" + f"${"Implicit?"}%-10s" + "\t" + f"${"Score"}%-10s" + "\n"
    val normalScore = scorer.getFullScoreForCluster(suggester.createSession(), isMoney = false)
    val implicitScore = scorer.getFullScoreForCluster(suggester.createImplicitSession(), isMoney = false)
    val body = f"${"Yes"}%-10s" + "\t" + f"$implicitScore%-10s" + "\n" + f"${"No"}%-10s" + "\t" + f"$normalScore%-10s"
    header + body
  }

  private def getParameterSets : Array[(Int, Int, Double, Double, Boolean)] = {
    val ranks = Array.apply(200, 300, 400)
    val numIterations = Array.apply(15, 20, 25)
    val lambdas = Array.apply(0.1, 0.5, 1.0)
    val alphas = Array.apply(0.01, 0.05, 0.1)
    val ratingMaxes = Array.apply(5.0, 10.0, 20.0, 50.0, 100.0)
    val implicitSets = for {lambda <- lambdas; alpha <- alphas; rank <- ranks; numIteration <- numIterations}
      yield (rank, numIteration, lambda, alpha, true)
    val nonImplicitSets = for {rank <- ranks; ratingMax <- ratingMaxes; numIteration <- numIterations; lambda <- lambdas}
      yield (rank, numIteration, lambda, ratingMax, false)
    implicitSets
  }
}
