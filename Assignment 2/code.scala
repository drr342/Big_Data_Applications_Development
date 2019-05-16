import scala.math.pow
import scala.math.sqrt

// PART B
val exchangeRate: Double = 0.88
// val dollars: Int = 100.00 - can't assign double to int
val dollars: Int = 100
var euros = 0.0
euros = dollars * exchangeRate
// dollars = 500 - can't modify immutable value
var dollars = 500
// dollars = 500.00 - can't assign double to int
var eurosInt: Int = 0
// eurosInt = dollars * exchangeRate - can't implicitly conver double2int
eurosInt = dollars * exchangeRate.toInt // eurosInt = 0
// The result in eurosInt is not useful because the rounding is performed at the exchangeRate level, i.e. exchangeRate (originally 0.88) becomes 0 and therefore the overall result is 0.

// PART C
eurosInt = (dollars * exchangeRate).toInt // eurosInt = 440
eurosInt.getClass // int
dollars.getClass // int
exchangeRate.getClass // double
println("$" + dollars + " = " + eurosInt + " Euros")
27/3.0
// res3 = 22.5 - can't reassign immutable value
pow(2,3) // 8
sqrt(64) // 8

// PART D
val record = "2017-01-08:10:00:00, 12345678-aaaa-1000-gggg-000111222333, 58, TRUE, enabled, disabled, 37.819722,-122.478611"
record.length // 109
record.contains("disabled") // true
record.indexOf("17") // 2
record.toLowerCase.indexOf("true") // 63
record
var record2 = record
record == record2 // true
record2 = "no match"
record == record2 // false