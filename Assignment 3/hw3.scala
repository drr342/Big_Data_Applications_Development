// BDAD HW3
// Daniel Rivera Ruiz
// drr342@nyu.edu

// 1) Create RDD
val mydata = sc.textFile("file:///home/drr342/bdad/hw3/frostroad.txt")
// 2) Count lines
mydata.count 
// 3) 
// res1 = 23
// 4) Collect (show contents of RDD)
mydata.collect


// 5) Create directory in HDFS
// hdfs dfs -mkdir bdad/hw3
// hdfs dfs -mkdir bdad/hw3/loudacre
// hdfs dfs -mkdir bdad/hw3/loudacre/weblog

// 6) Put the log file in HDFS
// hdfs dfs -put 2014-03-15.log bdad/hw3/loudacre/weblog/

// 7) See the HDFS version of the file
// hdfs dfs -cat bdad/hw3/

// 8) Initialize logfile
val logfile: String = "/user/drr342/bdad/hw3/loudacre/weblog/2014-03-15.log"
// 9) Creatre RDD
val logrdd = sc.textFile(logfile)
// 10) View 10 lines of the data
logrdd.take(10)
// 11) Create RDD with jpg requests
val jpgrdd = logrdd.filter(_.toLowerCase().contains("jpg"))
// 12) View 10 lines of jpgrdd
jpgrdd.take(10)
// 13) Count jpg requests
val jpgcount = logrdd.filter(_.toLowerCase().contains("jpg")).count
// 14) Create rdd with lengths of lines of the log file
val logLengthsrdd = logrdd.map(_.length)
// 15) Create rdd with arrays of words for each line of log file
val logWordsrdd = logrdd.map(_.split(" "))
// 16) Create RDD containing IP addresses
val ip = raw"(\d{1,3}\.){3}\d{1,3}".r
val logIPrdd = logrdd.map(ip.findFirstIn(_).getOrElse("No IP address found!"))
// 17) Print IP addresses
logIPrdd.collect.foreach(println)
// 18) Save list of IP addresses to HDFS directory
val ipfile: String = "/user/drr342/bdad/hw3/loudacre/iplist"
logIPrdd.saveAsTextFile(ipfile)
// 19) Show the contents of the directory
// hdfs dfs -ls bdad/hw3/loudacre/iplist

// 20) 
val weblogsfile: String = "/user/drr342/bdad/hw3/loudacre/weblogs/"
// 21)
val weblogsrdd = sc.textFile(weblogsfile)
// 22)
weblogsrdd.take(10)
// 23)
val webjpgrdd = weblogsrdd.filter(_.toLowerCase().contains("jpg"))
// 24)
webjpgrdd.take(10)
// 25)
val webjpgcount = weblogsrdd.filter(_.toLowerCase().contains("jpg")).count
// 26)
val weblogsLengthsrdd = weblogsrdd.map(_.length)
// 27)
val weblogsWordsrdd = weblogsrdd.map(_.split(" "))
// 28)
val weblogsIPrdd = weblogsrdd.map(ip.findFirstIn(_).getOrElse("No IP address found!"))
// 29)
weblogsIPrdd.collect.foreach(println)
// 30)
val bigipfile: String = "/user/drr342/bdad/hw3/loudacre/bigiplist"
weblogsIPrdd.saveAsTextFile(bigipfile)
// 31) 
// hdfs dfs -ls bdad/hw3/loudacre/bigiplist
// hdfs dfs -cat bdad/hw3/loudacre/bigiplist/part-00000 | head