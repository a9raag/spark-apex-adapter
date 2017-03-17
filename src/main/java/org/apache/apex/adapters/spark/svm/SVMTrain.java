package org.apache.apex.adapters.spark.svm;

import org.apache.apex.adapters.spark.ApexConf;
import org.apache.apex.adapters.spark.ApexContext;
import org.apache.apex.adapters.spark.ApexRDD;
import org.apache.apex.adapters.spark.properties.PathProperties;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.reflect.ClassTag;

/**
 * Created by krushika on 5/1/17.
 */
public class SVMTrain {
    public static void main(String[] args){
        PathProperties properties = new PathProperties();

        ApexContext sc= new ApexContext(new ApexConf().setMaster("local[2]").setAppName("Linear SVM"));
        ClassTag<LabeledPoint> tag = scala.reflect.ClassTag$.MODULE$.apply(LabeledPoint.class);
        String path = properties.getProperty("svmTrain");
        ApexRDD<LabeledPoint> data = new ApexRDD<>(MLUtils.loadLibSVMFile(sc, path),tag);
        int numIterations = 100;
        final SVMModel model = SVMWithSGD.train(data, numIterations);

// Clear the default threshold.
        model.clearThreshold();
        model.toPMML(properties.getProperty("SVMModelPath"));
    }
}
    