package comp9313.proj2

import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Problem2 {
  def main(args: Array[String]) = {
    val inputFile = args(0)
    val outputFolder = args(1)

    val conf = new SparkConf().setAppName("problem2")
    val sc = new SparkContext(conf)
    
    //iteration number
    val iterN = 4
    val edges = sc.textFile(inputFile)   
   
    //create graph base on file
    val edgelist = edges.map(x => x.split(" ")).map(x=> Edge(x(1).toLong, x(2).toLong, 0.0))	
    val graph = Graph.fromEdges[Double, Double](edgelist, 0.0)
	  graph.triplets.collect().foreach(println)  
    val initialGraph = graph.mapVertices((id, _) => "")

    //pregel operator
    val preG = initialGraph.pregel("", iterN)(

        // Vertex Program
        // keep new messages
        (id, dist, newDist) => newDist, 

        // Send Message
        // make message as string and concat with srcId
        triplet => {
            Iterator((triplet.dstId, triplet.srcAttr.split(",").map(x => x + "-" + triplet.srcId).mkString(",")))
        },
        //Merge Message
        //merge to a string
        (a, b) => a + "," + b )
       

    //process string to list
    val result = preG.vertices.map(x => {
      val id = x._1
      val rest = x._2.split(",").map(a => a.split("-").filter(a => a.length > 0).map(a => a.toInt))
      val rest2 = rest.filter(a => a.length > 0).filter(a =>  a(0) == a.last)
      (id, rest2)
    }
    )

    val result2 = result.map(x => {
      val id = x._1
      val rest = x._2.filter(a => a.length > 3).filter(a => a(1) == id).map(a => a.tail)
      (id, rest)

    }
    )
    //sorting result
    val sortResult = result.sortBy(a => a._1).map(a => (a._1, a._2.sortBy(in => (in(0), -in(1), -in(2))))).map(a => (a._1, a._2.map(in => in.mkString("->"))))

    val finalResult = sortResult.map(x => x._1 + ":" + x._2.mkString(";"))
    finalResult.map(x => s"${x}").saveAsTextFile(outputFolder)
  }
}
