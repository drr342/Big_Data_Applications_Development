// BDAD HW9_2
// Daniel Rivera Ruiz
// drr342@nyu.edu

val dir:String = "/user/drr342/bdad/hw9/accounts"

val challenge1 = sc.wholeTextFiles(dir).
					flatMap(account => account._2.split("\n")).
					keyBy(line => line.split(",")(8))

val challenge2 = challenge1.
					mapValues(line => (line.split(",")(4), line.split(",")(3))).
					groupByKey()

val challenge3 = challenge2.sortByKey()

challenge3.take(5).foreach { record =>
	println("--- " + record._1)
	record._2.foreach(println)
	println()
}