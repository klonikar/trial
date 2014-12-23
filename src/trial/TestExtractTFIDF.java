/**
* Assuming the ExtractTFIDF.scala has been compiled into tfidf_spark.jar,
* Compile using:
javac -d test -cp tfidf_spark.jar;<spar-path>\assembly\target\scala-2.10\spark-assembly-*.jar TestExtractTFIDF.java
* Run using:
java -cp test;tfidf_spark.jar;<spark-path>\assembly\target\scala-2.10\spark-assembly-*.jar -Dspark.executor.memory=11g -Xmx12g -Xms12g -Dspark.master=local[8] trial.TestExtractTFIDF
*/
package trial;

import java.io.*;
import scala.Tuple2;
import scala.Tuple4;

public class TestExtractTFIDF {
    public static void main(String[] args) throws IOException{
        String inputFile = "training_terms_emails.txt";
        int minDocCount = 10;
        int maxDocCount = 500;
        int maxTerms = 10000;
        double minInfoGainThreshold = 0.0005; // min improvement in entropy
        String outputFile = "training_terms_emails.out";
		boolean outputAsHDFS = false;
        Tuple2<String, Tuple4<Object,Object,Object,Object>>[][] output = 
                   ExtractTFIDF.execute(inputFile, minDocCount, maxDocCount, maxTerms,
                                        minInfoGainThreshold, outputFile, outputAsHDFS);
        if(!outputAsHDFS) {
            PrintWriter writer = new PrintWriter(new File(outputFile));
            for(Tuple2<String, Tuple4<Object,Object,Object,Object>>[] tfidfs : output) {
                String tfidfStr = "";
				for(Tuple2<String, Tuple4<Object,Object,Object,Object>> tfidf : tfidfs) {
                    tfidfStr += tfidf._1() + ":" + tfidf._2()._1() + ":" + tfidf._2()._2()
                                + ":" + tfidf._2()._3() + ":" + tfidf._2()._4() + ",";
                }
                writer.write(tfidfStr + "\n");
            }
		    writer.close();
        }
    }
}