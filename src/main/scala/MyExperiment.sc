def incrementValue(value:Int):Int = value+1;

def printIncrementValue(f: Int =>Int, initialValue : Int){ val incrementedValue = f(initialValue)
  println(s"The incremented value is $incrementedValue   ")
  }

printIncrementValue(incrementValue,10)