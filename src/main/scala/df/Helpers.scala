package df

object Helpers {
  def categorical[T](f: (Long, Int) => Long)(implicit cols: Seq[ColumnDef]): List[Any] => List[Any] = {
    return { l =>
      l
        .zipWithIndex
        .map {
          case (v, i) =>
            if (i> 0 && cols(i).isCategorical) f(v.asInstanceOf[Long], i) else v
        }
    }
  }
   def numerical[T](f: (Double, Int) => Double)(implicit cols: Seq[ColumnDef]): List[Any] => List[Any] = {
    return { l =>
      l
        .zipWithIndex
        .map {
          case (v, i) =>
            if (i >1 && !cols(i).isCategorical) f(v.asInstanceOf[Double], i) else v
        }
    }
  }

  def numericalWithType[T](f: (Double, ColumnDef) => Double)(implicit cols: Seq[ColumnDef]): List[Any] => List[Any] = {
    return { l =>
      l
        .zip(cols)
        .map {
          case (v, c) =>
            if (!c.isCategorical) f(v.asInstanceOf[Double], c) else v
        }
    }
  }

}
