/**
 * @author: Kiran Lonikar
 * Usage: ExtractTFIDF inputFile minDocCount maxDocCount minInfoGain outputFile
 * Compile standalone using scala 2.10(make sure you have built spark before that):
 * scalac -d . -cp 
   <spark_path>/assembly/target/scala-2.10/spark-assembly-*.jar
   ExtractTFIDF.scala
 * Execute using:
 * set SPARK_MEM=11g // needed only for spark-shell. deprecated but still works
 * java -cp .;<spark-path>/assembly/target/scala-2.10/spark-assembly-*.jar
 -Dspark.executor.memory=11g -Xmx12g -Xms12g -Dspark.master=local[8]
 trial.ExtractTFIDF --maxTerms 50000 --inputFile training_terms_emails.txt
 --outputFile training_terms_emails.out
 */

package trial

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import java.io._

object ExtractTFIDF {
  def main(args: Array[String]) {
    // Function parameters
    var inputFile = "training_terms_emails.txt"
    var minDocCount = 10
    var maxDocCount = 500
	var maxTerms = 10000
    var minInfoGainThreshold = 0.0005 // min improvement in entropy
    var outputFile = "training_terms_emails.out"
	var minPartitions = 48
	var outputAsHDFS = false
	
    (0 to (args.length-2)).map { i =>
        var argVal = args(i + 1)
        println("current arg: " + argVal)
        args(i) match {
            case "--inputFile" => inputFile = argVal
            case "--minDocCount" => minDocCount = argVal.toInt
            case "--maxDocCount" => maxDocCount = argVal.toInt
			case "--maxTerms" => maxTerms = argVal.toInt
            case "--minInfoGainThreshold" => minInfoGainThreshold = argVal.toDouble
            case "--outputFile" => outputFile = argVal
			case "--outputAsHDFS" => outputAsHDFS = true
			case "--minPartitions" => minPartitions = argVal.toInt
            case _ => ;
        }
    }

	val output = execute(inputFile, minDocCount, maxDocCount, maxTerms, minInfoGainThreshold,
                         outputFile, minPartitions, outputAsHDFS)
    if(!outputAsHDFS) {
        val writer = new PrintWriter(new File(outputFile))
        output.foreach {  tfidfs =>
            val tfidfStr = tfidfs.map { case (term, termStats) =>
                term + ":" + termStats(0) + ":" + termStats(1) + ":" + termStats(2) + ":" + termStats(3)
            }
            writer.write(tfidfStr.deep.mkString(",") + "\n")
        }
		writer.close
    }
  }
  
  def execute(inputFile: String, minDocCount: Int, maxDocCount: Int, maxTermsArg: Int,
              minInfoGainThreshold: Double, outputFile: String, minPartitions: Int, outputAsHDFS: Boolean) = {
    val sparkConf = new SparkConf().setAppName("ExtractTFIDF")
    val sc = new SparkContext(sparkConf)
    println("spark context created")

    val ret = executeWithSC(sc, inputFile, minDocCount, maxDocCount, maxTermsArg, minInfoGainThreshold,
                      outputFile, minPartitions, outputAsHDFS)
    sc.stop
	ret
  }
  
  def executeWithSC(sc: SparkContext, inputFile: String, minDocCount: Int, maxDocCount: Int, maxTermsArg: Int,
              minInfoGainThreshold: Double, outputFile: String, minPartitions: Int, outputAsHDFS: Boolean) = {

    val startTime = System.currentTimeMillis

val validLabels = List("Y", "N")
val validTermsLabels = sc.textFile(inputFile, minPartitions).map { line =>
    line.split(",")
    }.filter { fields => validLabels.contains(fields(0)) }.persist(StorageLevel.MEMORY_AND_DISK)

// compute the number of occurences for each label
val labelCounts = validTermsLabels.map { fields => (fields(0), 1)
}.reduceByKey(_ + _).collectAsMap
val labelCounts_broadcast = sc.broadcast(labelCounts)

val totalRows = labelCounts.reduce({ (a,b) => ("sum", a._2 + b._2) })._2

val probsOverAll = labelCounts.map { case(label, count) =>
    label -> (count.toDouble/totalRows) }
val originalEntropy = probsOverAll.reduce( {(a,b) =>
    ("entropy", -a._2*Math.log(a._2) - b._2*Math.log(b._2)) })._2

// Compute the number of occurences for each term in the docs
// and also the term, label occurences
val labelTermsDocCounts = validTermsLabels.flatMap { fields =>
    val label = fields(0)
    val terms = fields.slice(1, fields.length).distinct
    terms.map { term => (term, (1, Map(((label, term), 1)))) } 
}.reduceByKey { (a,c) => 
       (a._1 + c._1, a._2 ++ c._2.map { case(tuple, count) => // merge the maps for term, label
           tuple -> (count + a._2.getOrElse(tuple, 0)) } )
    }.filter { tlc =>
        tlc._2._1 > minDocCount && tlc._2._1 < maxDocCount
    }

val infoGainForTerms = labelTermsDocCounts.map { tldc =>
    val numDocsWithTerm = tldc._2._1
    val labelCountsWithTerm = tldc._2._2
	val labelCountsWithoutTerm = labelCounts_broadcast.value.map { case(label, count) =>
        (label, labelCountsWithTerm.head._1._2) -> 
            (count - labelCountsWithTerm.getOrElse((label, labelCountsWithTerm.head._1._2), 0))
        }
    val probsWithTerm = labelCountsWithTerm.map { case((label,term), count) =>
        (label, term) -> (count.toDouble/numDocsWithTerm)
    }
    val probsWithoutTerm = labelCountsWithoutTerm.map { case((label,term), count) =>
        (label, term) -> (count.toDouble/(totalRows - numDocsWithTerm))
    }
    val weightedEntropyWithTerm = numDocsWithTerm*probsWithTerm.reduce( {(a,b) =>
        val x1 = if(a._2 > 0.0) -a._2*Math.log(a._2) else 0.0
		val x2 = if(b._2 > 0.0) -b._2*Math.log(b._2) else 0.0
		(("entropy", labelCountsWithTerm.head._1._2), x1 + x2) })._2/totalRows
    val weightedEntropyWithoutTerm = (totalRows - numDocsWithTerm)*probsWithoutTerm.reduce(
        {(a,b) =>
            val x1 = if(a._2 > 0.0) -a._2*Math.log(a._2) else 0.0
		    val x2 = if(b._2 > 0.0) -b._2*Math.log(b._2) else 0.0
		    (("entropy", labelCountsWithTerm.head._1._2), x1 + x2)
        })._2/totalRows
    val entropyInclTerm = weightedEntropyWithTerm + weightedEntropyWithoutTerm
    val infoGain = (originalEntropy - entropyInclTerm) / originalEntropy
	(labelCountsWithTerm.head._1._2, infoGain)
    }.filter { termInfoGain => termInfoGain._2 >= minInfoGainThreshold
    }

val allInfoGainDocCounts = labelTermsDocCounts.join(infoGainForTerms).map { tldc =>
    (tldc._1, (tldc._2._1._1, tldc._2._2)) // (term, (docCount, infoGain))
    }

val maxTerms = if(maxTermsArg > 0) maxTermsArg else allInfoGainDocCounts.count.toInt
// select top maxTerms terms based on infoGain
val infoGainDocCounts = allInfoGainDocCounts.top(maxTerms)(
                          Ordering[Double].on(_._2._2) // order on infoGain
                        ).toMap
/*
val infoGainDocCounts = allInfoGainDocCounts.sortBy({ termInfoGain =>
        termInfoGain._2._2}, false).take(maxTerms).toMap
*/
val infoGainDocCounts_broadcast = sc.broadcast(infoGainDocCounts)

val termFrequencies = validTermsLabels.map { fields =>
    val terms = fields.slice(1, fields.length)
    val distinctTerms = terms.distinct.filter( { term => infoGainDocCounts_broadcast.value.contains(term) } )
    distinctTerms.map { term =>
	    val termCountsInDoc = terms.count(tt => tt == term)
		val docCountIGForTerm = infoGainDocCounts_broadcast.value(term) // doc count, infoGain
        // return a term, TF, TFIDF, TFLOGIDF, infoGain
        (term, Array(termCountsInDoc, 
                termCountsInDoc.toDouble/docCountIGForTerm._1,
                termCountsInDoc.toDouble*Math.log(totalRows.toDouble/docCountIGForTerm._1),
                docCountIGForTerm._2))
    }
}
if(outputAsHDFS) {
    val termFrequenciesOutput = termFrequencies.map { tfidfs =>
        val tfidfStr = tfidfs.map { case (term, termStats) => 
          term + ":" + termStats(0) + ":" + termStats(1) + ":" + termStats(2) + ":" + termStats(3)
        }
        tfidfStr.deep.mkString(",")
    }
    termFrequenciesOutput.saveAsTextFile(outputFile)
}
val ret = termFrequencies.collect

val endTime = System.currentTimeMillis
println("time to execute: " + (endTime - startTime) + "ms");
ret
}
}
