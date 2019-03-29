package cads

object ConversionFunctions {
  def fromLineToTrip(line: String): Trip = {
    val words = line.split(" ")
    Trip(words(0), words(1), words(2).toInt)
  }
}
