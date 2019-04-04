package com.examplecompany.sales

/**
  * Writer of a generic progress bar in increments of 5%.
  */
class ProgressBarWriter(private val header : String, private val finishPoint : Double) {
  private var lastProgress = 0.0

  /**
    * Calculates the beginning of the progress bar to be displayed during analysis.
    * @return The string representing what should be conveyed to the user.
    */
  def startProgressBar() : String = {
    header + "\n" + "["
  }

  /**
    * Updates the progress bar based on the current progress.
    * @param currentProgress The current value of progress. Must be between 0 and finishPoint.
    * @return A string representing what should be added to the display to the user.
    */
  def updateProgressBar(currentProgress : Double) : String = {
    val percentDone = currentProgress / finishPoint * 100.0
    val oldPercentDone  = lastProgress / finishPoint * 100.0
    val numStars = percentDone.toInt / 5 - oldPercentDone.toInt / 5
    lastProgress = currentProgress
    "*" * numStars
  }

  /**
    * Calculates the end of the progress bar to be displayed during analysis.
    * @return The string representing what should be conveyed to the user at the end of the progress bar.
    */
  def finishProgressBar() : String = {
    "]\n"
  }
}
