package comp9313.proj2

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object Problem1 {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFolder = args(1)
    val conf = new SparkConf().setAppName("problem1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(inputFile)


    val combinations = textFile.flatMap {line => {
        //tokenize
        val items = line.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+").filter(x => x.length >= 1).map(x => x.toLowerCase).filter(x => x.charAt(0) <='z' && x.charAt(0) >='a').toList
        //create string pair
        val pairs = for{i <- 0 until items.length; j <- i + 1 until items.length} yield (items(i), items(j))
        pairs.toList
        //create string pair with "*"
        val pairs2 = for{i <- 0 until items.length; j <- i + 1 until items.length} yield (items(i), "*")
        pairs2.toList
        pairs ++ pairs2
      }
    }

    //combiner
    val combiner = combinations.map(item => item -> 1).reduceByKey(_ + _).sortByKey()


    var marginal = 0.0

    //reducer
    val reducer = combiner.map(entry => {
        entry._1 match {
          case (_, "*") => {
              marginal = entry._2
              (entry._1, entry._2)
          }
          case (_, _) => {
              (entry._1, entry._2 / marginal)
          }
        }
      }
    )

    //process result
    val finalResult = reducer.filter(x => !x._1._2.contains("*"))
    	.map(x => (x._1._1, x._1._2, x._2.asInstanceOf[Number].doubleValue))
    	.sortBy(x => (x._1, -x._3, x._2))

    //val xx = fi.map(x => (x._1._1, x._1._2, x._2.asInstanceOf[Number].doubleValue))

    //val xxx = xx.sortBy(x => (x._1, -x._3, x._2))

    finalResult.map(x => s"${x._1} ${x._2} ${x._3}").saveAsTextFile(outputFolder)


  }
}
