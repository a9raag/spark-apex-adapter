package org.apache.apex.adapters.spark.linearregression;

import org.apache.apex.adapters.spark.properties.PathProperties;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import scala.Tuple2;

/**
 * Created by krushika on 22/2/17.
 */
public class LinearRegressionTest {
    public static void main(String[] args) {
        PathProperties properties = new PathProperties();
        SparkConf conf = new SparkConf().setAppName("Linear Regression Test").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // Load and parse the data
        String path = properties.getProperty("lrTest");
        JavaRDD<String> data = jsc.textFile(path,1);
        JavaRDD<LabeledPoint> parsedData = data.map(
                new Function<String, LabeledPoint>() {
                    public LabeledPoint call(String line) {
                        String[] parts = line.split(",");
                        String[] features = parts[1].split(" ");
                        double[] v = new double[features.length];
                        for (int i = 0; i < features.length - 1; i++)
                            v[i] = Double.parseDouble(features[i]);
                        return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(v));
                    }
                }
        );

        final LinearRegressionModel model = LinearRegressionModel.load(jsc.sc(),properties.getProperty("LinearRegressionModelPath"));

        JavaRDD<Tuple2<Double, Double>> valuesAndPreds = parsedData.map(
                new Function<LabeledPoint, Tuple2<Double, Double>>() {
                    public Tuple2<Double, Double> call(LabeledPoint point) {
                        double prediction = model.predict(point.features());
                        return new Tuple2<Double, Double>(prediction, point.label());
                    }
                }
        );
        Double MSE = new JavaDoubleRDD(valuesAndPreds.map(
                new Function<Tuple2<Double, Double>, Object>() {
                    public Object call(Tuple2<Double, Double> pair) {
                        return Math.pow(pair._1() - pair._2(), 2.0);
                    }
                }
        ).rdd()).mean();

        System.out.println("training Mean Squared Error = " + MSE);
    }


}
