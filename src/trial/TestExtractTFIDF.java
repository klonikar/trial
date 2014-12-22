/**
* Assuming the ExtractTFIDF.scala has been compiled into tfidf_spark.jar,
* Compile using:
javac -d test -cp tfidf_spark.jar;<spar-path>\assembly\target\scala-2.10\spark-assembly-*.jar TestExtractTFIDF.java
* Run using:
set SPARK_MEM=2048M // deprecated but still works
java -cp test;tfidf_spark.jar;<spark-path>\assembly\target\scala-2.10\spark-assembly-*.jar -Dspark.executor.memory=1536m -Xmx2048M -Xms2048M -Dspark.master=local[4] com.informatica.prototype.cto.mlp.preprocessing.TestExtractTFIDF
*/
package com.informatica.prototype.cto.mlp.preprocessing;

public class TestExtractTFIDF {
    public static void main(String[] args) {
        String inputFile = "C:/p4/kl_mlp/new_mlp/gcs/training_terms_emails.txt";
        int minDocCount = 10;
        int maxDocCount = 500;
        int maxTerms = 10000;
        double minInfoGainThreshold = 0.0005; // min improvement in entropy
        String outputFile = "C:/p4/kl_mlp/new_mlp/gcs/training_terms_case_title.out";
        ExtractTFIDF.execute(inputFile, minDocCount, maxDocCount, maxTerms, minInfoGainThreshold, outputFile);
    }
}