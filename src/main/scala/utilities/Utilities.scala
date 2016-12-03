package utilities

/**
  * Created by Satya on 03/12/2016.
  */
object Utilities {
  implicit class StringUtilities(val value: String) extends AnyVal {
    def isNullOrEmpty(x: String) = x == null || x.isEmpty
  }

}
