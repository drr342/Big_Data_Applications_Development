// BDAD HW4
// Daniel Rivera Ruiz
// drr342@nyu.edu

import java.text.SimpleDateFormat

val fileIn: String = "/user/drr342/bdad/hw4/devicestatus.txt"
val rdd = sc.textFile(fileIn)
val rddSplit = rdd.map(line => line.split(line.charAt(19)))
val rddFilter = rddSplit.filter(_.length == 14)
val dateFormat = new SimpleDateFormat("yyyy-MM-dd:hh:mm:ss")
val rddTuple = rddFilter.map(record => (dateFormat.parse(record(0)), 
											record(1).split(" ")(0), 
											record(2), 
											record(12).toDouble, 
											record(13).toDouble))
val fileOut: String = "/user/drr342/bdad/hw4/devicestatus_etl"
rddTuple.map(tuple => tuple.toString.replaceAll("[()]","")).saveAsTextFile(fileOut)
