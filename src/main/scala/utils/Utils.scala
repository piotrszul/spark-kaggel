package utils

import java.io.Closeable
import java.io.ObjectInputStream
import java.io.FileInputStream
import java.io.ObjectOutputStream
import java.io.FileOutputStream

object Utils {
  
  def use[F <: Closeable,T](file:F)(handler:(F=>T)):T = {
    try {
      return handler(file)
    } finally {
      file.close()
    }
  }
  
  def readObject[T](filename:String):T = use(new ObjectInputStream(new FileInputStream(filename))) { f =>
     f.readObject().asInstanceOf[T]
   }
  
  def writeObject[T <: Serializable](filename:String)(obj:T) = use(new ObjectOutputStream(new FileOutputStream(filename))) { f =>
     f.writeObject(obj)
   }
  
}