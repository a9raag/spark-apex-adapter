package org.apache.apex.adapters.spark.chisquareselector;


import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.apex.adapters.spark.ApexConf;
import org.apache.apex.adapters.spark.ApexContext;
import org.apache.apex.adapters.spark.ApexRDD;
import org.apache.apex.adapters.spark.properties.PathProperties;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.feature.ChiSqSelector;
import org.apache.spark.mllib.feature.ChiSqSelectorModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.Function1;
import scala.reflect.ClassTag;

import java.io.Serializable;

/**
 * Created by harsh on 17/12/16.
 */
@DefaultSerializer(JavaSerializer.class)
public class TestChiSqSelector implements Serializable {
    public static Function1 f;
    public TestChiSqSelector(){

    }
    public TestChiSqSelector(ApexContext sc){
        PathProperties properties = new PathProperties();
        String path = properties.getProperty("chiTrainDataHDFS");

        ClassTag<LabeledPoint> tag = scala.reflect.ClassTag$.MODULE$.apply(LabeledPoint.class);
        ApexRDD<LabeledPoint> inputData = new ApexRDD<LabeledPoint> (MLUtils.loadLibSVMFile(sc, path,-1,1), tag);
        ChiSqSelector selector = new ChiSqSelector(50);
        ApexRDD discretizedData = (ApexRDD) inputData.map(new Function<LabeledPoint, LabeledPoint>() {
            @Override
            public LabeledPoint call(LabeledPoint lp) {
                final double[] discretizedFeatures = new double[lp.features().size()];
                for (int i = 0; i < lp.features().size(); ++i) {
                    discretizedFeatures[i] = Math.floor(lp.features().apply(i) / 16);
                }
                return new LabeledPoint(lp.label(), Vectors.dense(discretizedFeatures));
            }
        });

        final ChiSqSelectorModel transformer = selector.fit(discretizedData);

        ApexRDD<LabeledPoint> filteredData = (ApexRDD<LabeledPoint>) discretizedData.map(
                new Function<LabeledPoint, LabeledPoint>() {
                    @Override
                    public LabeledPoint call(LabeledPoint lp) {
                        return new LabeledPoint(lp.label(), transformer.transform(lp.features()));
                    }
                }
        );
        System.out.println(transformer.formatVersion()+" ::"+ filteredData.hashCode());

        filteredData.foreach(new VoidFunction<LabeledPoint>() {
            @Override
            public void call(LabeledPoint v1) throws Exception {
                System.out.println(v1);
            }
        });

    }
    public static void main(String args[]){
        ApexContext sc  = new ApexContext(new ApexConf().setMaster("local").setAppName("ApexApp_ChiSquare"));
        TestChiSqSelector t = new TestChiSqSelector(sc);
    }

}
