/**
 * @author: Kiran Lonikar
 * Usage: ExtractTFIDF inputFile minDocCount maxDocCount minInfoGain outputFile
 * Compile standalone using scala 2.10(make sure you have built spark before that):
 * scalac -d . -cp 
   <spark_path>/assembly/target/scala-2.10/spark-assembly-*.jar
   ExtractTFIDF.scala
 * Execute using:
 * set SPARK_MEM=2048M // deprecated but still works
 * java -cp .;<spark_path>/assembly/target/scala-2.10/spark-assembly-*.jar 
  -Dspark.executor.memory=1536m -Xmx2048M -Xms2048M -Dspark.master=local[4]
  com.informatica.prototype.cto.mlp.preprocessing.ExtractTFIDF
  --inputFile C:/p4/kl_mlp/new_mlp/gcs/training_terms_emails.txt
 */

package com.informatica.prototype.cto.mlp.preprocessing

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

object ExtractTFIDF {
  def main(args: Array[String]) {
    // Function parameters
    var inputFile = "C:/p4/kl_mlp/new_mlp/gcs/training_terms_emails.txt"
    var minDocCount = 10
    var maxDocCount = 500
	var maxTerms = 10000
    var minInfoGainThreshold = 0.0005 // min improvement in entropy
    var outputFile = "C:/p4/kl_mlp/new_mlp/gcs/training_terms_case_title.out"
	
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
            case _ => ;
        }
    }

	execute(inputFile, minDocCount, maxDocCount, maxTerms, minInfoGainThreshold, outputFile)
  }
  
  def execute(inputFile: String, minDocCount: Int, maxDocCount: Int, maxTermsArg: Int,
              minInfoGainThreshold: Double, outputFile: String) = {

    val sparkConf = new SparkConf().setAppName("ExtractTFIDF")
    val sc = new SparkContext(sparkConf)
    println("spark context created")
    val startTime = System.currentTimeMillis

val validLabels = List("Y", "N")
val validTermsLabels = sc.textFile(inputFile).map { line =>
    line.split(",")
    }.filter { fields => validLabels.contains(fields(0)) }.persist(StorageLevel.MEMORY_AND_DISK)

// compute the number of occurences for each label
val labelCounts = validTermsLabels.map { fields => (fields(0), 1)
}.reduceByKey(_ + _).collectAsMap

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

val totalRows = labelCounts.reduce({ (a,b) => ("sum", a._2 + b._2) })._2

val probsOverAll = labelCounts.map { case(label, count) =>
    label -> (count.toDouble/totalRows) }
val originalEntropy = probsOverAll.reduce( {(a,b) =>
    ("entropy", -a._2*Math.log(a._2) - b._2*Math.log(b._2)) })._2

val infoGainForTerms = labelTermsDocCounts.map { tldc =>
    val numDocsWithTerm = tldc._2._1
    val labelCountsWithTerm = tldc._2._2
	val labelCountsWithoutTerm = labelCounts.map { case(label, count) =>
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

val termFrequencies = validTermsLabels.map { fields =>
    val terms = fields.slice(1, fields.length)
    val distinctTerms = terms.distinct.filter( { term => infoGainDocCounts.contains(term) } )
    distinctTerms.map { term =>
	    val termCountsInDoc = terms.count(tt => tt == term)
		val docCountIGForTerm = infoGainDocCounts(term) // doc count, infoGain
        // return a map of term -> (TF, TFIDF, TFLOGIDF)
        (term, (termCountsInDoc, 
                termCountsInDoc.toDouble/docCountIGForTerm._1,
                termCountsInDoc.toDouble*Math.log(totalRows.toDouble/docCountIGForTerm._1),
                docCountIGForTerm._2))
    }
}

val termFrequenciesOutput = termFrequencies.map { tfidfs =>
    val output = tfidfs.map { tfidf => // term:termCount:tfIDF:tfLogIDF:infoGain
      tfidf._1 + ":" + tfidf._2._1 + ":" + tfidf._2._2 + ":" + tfidf._2._3 + ":" + tfidf._2._4
    }
    output.deep.mkString(",")
}
termFrequenciesOutput.saveAsTextFile(outputFile)
val endTime = System.currentTimeMillis
println("time to execute: " + (endTime - startTime) + "ms");
sc.stop
}
}