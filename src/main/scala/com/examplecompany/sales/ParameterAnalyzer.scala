package com.examplecompany.sales

/**
  * Trait which is used by various analyzers for different techniques and sets of parameters.
  */
trait ParameterAnalyzer {
  /**
    * Calculates the beginning of the progress bar to be displayed during analysis.
    * @param header The header that should be displayed before the progress bar.
    * @return The string representing what should be conveyed to the user.
    */
  protected def startProgressBar(header : String) : String = {
    header + "\n" + "["
  }

  /**
    * Updates the progress bar based on the current progress.
    * @param currentProgress The current value of progress. Must be between 0 and finishPoint.
    * @param lastProgress The last value which was passed into this function as currentProgress. 0 <= lastProgress <= currentProgress.
    * @param finishPoint The value at which progress is said to be complete.
    * @return A string representing what should be added to the display to the user.
    */
  protected def updateProgressBar(currentProgress : Double, lastProgress : Double, finishPoint : Double) : String = {
    val percentDone = currentProgress / finishPoint * 100.0
    val oldPercentDone  = lastProgress / finishPoint * 100.0
    val numStars = percentDone.toInt / 5 - oldPercentDone.toInt / 5
    "*" * numStars
  }

  /**
    * Calculates the end of the progress bar to be displayed during analysis.
    * @return The string representing what should be conveyed to the user at the end of the progress bar.
    */
  protected def finishProgressBar() : String = {
    "]\n"
  }
}