//z5169989
package comp9313.proj3

import java.util.function.ToLongFunction
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


class SecondarySortKey(val second:Double, val first:Int) extends Ordered[SecondarySortKey] with Serializable
{
  def compare(that:SecondarySortKey):Int =
  {
    if(this.first - that.first != 0) {
      this.first.compareTo(that.first)
    }
    else {
      this.second.compareTo(that.second)
    }
  } 
}
 

object SetSimJoin {

	def main(args: Array[String]) {
		val input = args(0)
	    val ouput = args(1)
	    val thres = args(2).toDouble
	    val conf = new SparkConf().setAppName("SetSimJoin")

	    //processing input
	    val sc = new SparkContext(conf)
    	val lines = sc.textFile(input).flatMap(line => line.split("\n"))
    	//split
    	val tempL = lines.map(a => a.drop(a.split(" ")(0).length.toInt + 1))
    	//count each element
    	val ele = tempL.flatMap( a => a.split(" ") ).map ( a => (a, 1) )
    				.reduceByKey(_+_)

    	//sort by frequency
    	val tok = ele.map(a => (a._2, a._1)).sortByKey().map(a => (a._2, a._1))
    				.map(l => (new SecondarySortKey(l._1.toDouble, l._2.toInt),l))
    				.sortByKey().map(a => a._2._1)


    	val sortL = lines.map( l => l.split(" ") ).flatMap ( a => {
    		var ID = a(0)
            var line_array = a.drop(1).sortBy(tokens => tokens)
            val prefix_length = (line_array.length - Math.ceil(line_array.length * thres) + 1).toInt
            for(i <- 0 until prefix_length) yield(line_array(i), (ID, line_array))
            }
        ).groupByKey().map( a => {(a._1,a._2.toArray)})



        val sortP = sortL.flatMap(a => {
        	var result: Map[(String, String),Double] = Map()
        	for(i <- 0 until a._2.size) {
        		for(j <- i until a._2.size) {
        			if(i != j) {
        				val intersect = (a._2(i)._2.toSet & a._2(j)._2.toSet).size.toDouble
                        val union = (a._2(i)._2.toSet.size + a._2(j)._2.toSet.size).toDouble - intersect
                        val division = intersect/union

                        if(division >= thres) {
                        	if(!result.contains((a._2(i)._1, a._2(j)._1))) {
                        		result += ((a._2(i)._1, a._2(j)._1) -> division)
                        	}
                        }
        			}

        		}
        	}
        	result
        }
        )

        //process output
        sortP.distinct().map(l => (new SecondarySortKey(l._1._2.toDouble, l._1._1.toInt),l))
        .sortByKey().map(a =>a._2._1+"\t"+a._2._2).saveAsTextFile(ouput)
	}
}




//question 2

class Pair
	String term
	int value

class Mapper

	//the input is 1 document in dataset
	method Map(Object, Text)
		foreach term in Text
			emit Pair(term, 1)//the output is (term, 1)

class Reducer
	// the queue to get all Pair
	PriorityQueue<Pair> queue 
	// number of top frequent term
	int k 

	// to sort the output
	method Comparator<Pair> pC (P1, P2)
		//compare frequency
		if P1.value != P2.value
			return P1.value - P2.value
		//compare if equal frequency
		return P1.term.compare(P2.term)

	//reduce
	method Reduce(String term, Interator<integer> values)
		//the input like (term, [1,1,1...])
		foreach val in values
			sum += val

		p_temp = Pair(term, sum)

		if(queue.length < k)
			queue.add(p_temp)
		else
			Pair peek = queue.peak
			if(pC(p_temp, peek) > 0)
				queue.poll
				queue.add(p_temp)

	method cleanup(Output(String, int) output)
		List<Pair> p_t 
		foreach ele in queue
			p_t.add(queue.poll)

		foreach i from tail to head in p_t 
			output.add(i)



//question 3a

object BooleanInvertedIndex {

  def main(args: Array[String]) {
  	val input = args(0)
	val ouput = args(1)

	val lines = sc.textFile(input).getLines().toList

	//seperate term by space in each line
	val temp = lines.map(_.split(" "))

	//the flm is (term, docID)
	val flm = temp.flatMap(a => a.drop(1).map(b => (b, a(0))))

	//result is (term, [docID1, docID2...])
	val result = flm.groupBy(_._1).map(p => (p._1, p._2.map(_._2).toVector)).saveAsTextFile(ouput)

  }
}

//question 3b

val pairs = sc. parallelize(List((1, 2, 5), (1, 4, 6), (3, 4, 2), (3, 6, 8) … …))
val sourceNode = s.toInt
val edgeList = pairs.map( x => Edge(x._1.toLong, x._2.toLong, x._3.toDouble))
val graph = Graph.fromEdges[Int, Double](edgelist, 0)
// fill your code here, and store the result in a variable numRNodes



println(numRNodes)

object SSSP {
	def main(args: Array[String]) {
		val pairs = sc. parallelize(List((0,1, 10.0), (0, 2, 5.0), (1, 2, 2.0), (1, 3, 1.0), (2, 1, 3.0), (2, 3, 9.0), (2, 4, 2.0), (3, 4, 4.0), (4, 0, 7.0), (4, 3, 5.0)))
		val sourceNode = 0
		val edgeList = pairs.map( x => Edge(x._1.toLong, x._2.toLong, x._3.toDouble))
		val graph = Graph.fromEdges[Int, Double](edgelist, 0)
		// fill your code here, and store the result in a variable numRNodes

		val initialGraph = graph.mapVertices((id, _) => if (id == sourceNode) 0.0 else Double.PositiveInfinity)

		val res = initialGraph.pregel(Double.PositiveInfinity)(

        // Vertex Program
        (id, dist, newDist) => math.min(dist, newDist), 

        // Send Message

        triplet => { // Send Message
			if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
				Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
			} else { Iterator.empty }
		},

        //Merge Message
        (a, b) => math.min(a, b) )

		println(res.vertices.size)

	}
}
