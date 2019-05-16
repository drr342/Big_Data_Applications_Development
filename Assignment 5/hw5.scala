// BDAD HW5
// Daniel Rivera Ruiz
// drr342@nyu.edu

import scala.xml._

// Given a string containing XML, parse the string, and
// return an iterator of activation XML records (Nodes) contained in the string
def getactivations(xmlstring: String): Iterator[Node] = {
	val nodes = XML.loadString(xmlstring).\\("activation")
	nodes.toIterator
}

// Given an activation record (XML Node), return the model name
def getmodel(activation: Node): String = {
	(activation.\("model")).text
}

// Given an activation record (XML Node), return the account number
def getaccount(activation: Node): String = {
	(activation.\("account-number")).text
}

val dir:String = "/user/drr342/bdad/hw5/activations"
val xml_files = sc.wholeTextFiles(dir)
val nodes_it = xml_files.map(f => getactivations(f._2))
val nodes_list = nodes_it.map(it => it.toList).flatMap(identity)
val nodes_tuples = nodes_list.map(node => (getaccount(node), getmodel(node)))
val nodes_strings = nodes_tuples.map(tup => String.format("%s:%s", tup._1, tup._2))

val fileOut: String = "/user/drr342/bdad/hw5/account-models"
nodes_strings.saveAsTextFile(fileOut)