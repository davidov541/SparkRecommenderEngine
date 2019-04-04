package com.examplecompany.sales.GraphFrames

import com.examplecompany.sales.{ParameterAnalyzer, DataSet, Scorer}

/**
  * Created by davidmcginnis on 3/13/17.
  */
class GraphFramesAccuracyAnalyzer(private val dataSet : DataSet, seed : Long, suggester: GraphFramesSuggester) extends ParameterAnalyzer {

  def runAnalysis(scorer : Scorer) : String = {
    val header = "Evaluating Graph Frames Accuracy\n" + f"${"Score"}%-10s" + "\n"
    val scoreString = scorer.getFullScoreForCluster(suggester.createSession(dataSet), isMoney = true)
    val body = f"$scoreString%-10s"
    header + body
  }
}