package kaggel

import df.VarType

object KaggelIO {
	def typeGuesser(i:Int,n:String) = 
	  if (n.startsWith("C") || i == 0) VarType.Categorical else VarType.Numerical
}