package com.examplecompany.sales.MBA

import com.examplecompany.sales.{ParameterAnalyzer, DataSet, Scorer}

/**
  * Created by davidmcginnis on 3/13/17.
  */
class MBAAccuracyAnalyzer(private val dataSet : DataSet, seed : Long, suggester: MBASuggester) extends ParameterAnalyzer {

  def runAnalysis(scorer : Scorer) : String = {
    val header = "Evaluating MBA Accuracy\n" + f"${"Score"}%-10s" + "\n"
    val scoreString = scorer.getFullScoreForCluster(suggester.createSession(dataSet), isMoney = false)
    val body = f"$scoreString%-10s"
    header + body
  }
}
