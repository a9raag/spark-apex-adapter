package com.datatorrent.example.apexsvm;

import com.datatorrent.example.ApexConf;
import com.datatorrent.example.ApexContext;
import com.datatorrent.example.ApexRDD;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.reflect.ClassTag;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by krushika on 5/1/17.
 */
public class SVMTrain {
    public static void main(String[] args){
        Properties properties = new Properties();
        InputStream input;
        try{
            input = new FileInputStream("/home/anurag/spark-apex/spark-example/src/main/java/com/datatorrent/example/properties/svm.properties");
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ApexContext sc= new ApexContext(new ApexConf().setMaster("local[2]").setAppName("Linear SVM"));
        ClassTag<LabeledPoint> tag = scala.reflect.ClassTag$.MODULE$.apply(LabeledPoint.class);
        ApexRDD<LabeledPoint> data = new ApexRDD<>(MLUtils.loadLibSVMFile(sc, "/user/anurag/sample_libsvm_data.txt"),tag);

        int numIterations = 100;
        final SVMModel model = SVMWithSGD.train(data, numIterations);

// Clear the default threshold.
        model.clearThreshold();
        model.save(sc,properties.getProperty("SVMModelPath"));
    }
}