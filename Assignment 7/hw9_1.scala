// BDAD HW9_1
// Daniel Rivera Ruiz
// drr342@nyu.edu

val log:String = "/user/drr342/bdad/hw9/2014-03-15.log"
val dir:String = "/user/drr342/bdad/hw9/accounts"

val userData = sc.wholeTextFiles(dir).
					flatMap(account => account._2.split("\n")).
					map(line => line.split(",")).
					map(arr => (arr(0), arr))

val hitCount = sc.textFile(log).
					map(line => (line.split(" ")(2), 1)).
					reduceByKey(_ + _)

val joined = userData.join(hitCount)

joined.take(5).foreach(record => println(record._1, record._2._2, record._2._1(3), record._2._1(4)))