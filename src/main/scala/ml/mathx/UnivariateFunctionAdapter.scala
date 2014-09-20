package ml.mathx

import org.apache.commons.math3.analysis.UnivariateFunction


class UnivariateFunctionAdapter(val f:Double => Double) extends UnivariateFunction {
  
  override def value(v:Double):Double = { println(v); 
  f(v)}
}
