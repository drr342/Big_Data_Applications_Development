// BDAD HW6
// Daniel Rivera Ruiz
// drr342@nyu.edu

val file:String = "/user/drr342/bdad/hw6/2014-03-15.log"
val dir:String = "/user/drr342/bdad/hw6/accounts"

val log = sc.textFile(file)
val setupCountsRDD = log.map(line => (line.split(" ")(2), 1))
val requestCountRDD = setupCountsRDD.reduceByKey(_ + _)
val visitFrequencyTotalsRDD = requestCountRDD.map(user => (user._2, 1)).reduceByKey(_ + _)

val accounts = sc.wholeTextFiles(dir).
					flatMap(account => account._2.split("\n")).
					map(line => (line.split(",")(0).toInt, Unit))
val ips = log.map(line => line.split(" ")).
					map(tokens => (tokens(2).toInt, tokens(0))).
					groupByKey.
					map(tup => (tup._1, tup._2.toList))
val validAcctsIpsFinalRDD = accounts.join(ips).map(tuple => (tuple._1, tuple._2._2))