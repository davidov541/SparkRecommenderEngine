package com.examplecompany.sales.MBA

import com.examplecompany.sales.DataSet

/**
  * Created by davidmcginnis on 3/13/17.
  */
class MBASuggester {

  def createSession(dataset : DataSet) : MBASession = new MBASession(dataset)
}
