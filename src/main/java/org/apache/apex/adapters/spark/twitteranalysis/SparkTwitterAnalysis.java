package org.apache.apex.adapters.spark.twitteranalysis;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.apache.apex.adapters.spark.ApexConf;
import org.apache.apex.adapters.spark.ApexContext;
import org.apache.apex.adapters.spark.ApexRDD;
import org.apache.apex.adapters.spark.io.ReadFromFS;
import org.apache.apex.adapters.spark.io.WriteToFS;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import twitter4j.*;

import java.io.FileWriter;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by krushika on 17/3/17.
 */
public class SparkTwitterAnalysis {

    public static ArrayList<String> getTweets(String topic) {

        Twitter twitter = new TwitterFactory().getInstance();
        ArrayList<String> tweetList = new ArrayList<String>();
        try {
            Query query = new Query(topic);
            QueryResult result;
            do {
                result = twitter.search(query);
                List<Status> tweets = result.getTweets();
                for (Status tweet : tweets) {
                    tweetList.add(tweet.getText());
                }
            } while ((query = result.nextQuery()) != null);
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to search tweets: " + te.getMessage());
        }
//        System.out.println("The tweets are "+tweetList);
        return tweetList;
    }

    public static void processingTweets(String tweet){
        Properties prop = new Properties();
        prop.setProperty("annotators", "tokenize, ssplit, parse, sentiment, lemma, ner");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(prop);
//        String text = " I want to kill him, he is the most disgusting person, betrayal";
        Annotation doc = new Annotation(tweet);
        pipeline.annotate(doc);
        List<CoreMap> sentences = doc.get(CoreAnnotations.SentencesAnnotation.class);
        for (CoreMap sentence : sentences){
            Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
            int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)){
                String ne = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);
//                System.out.println("Named Entity Annotation : "+ne);
            }
            System.out.println("Sentence :"+sentence+" Sentiment :"+sentiment);
        }
    }

    public static void main(String[] args){
        ArrayList<String> list = getTweets("cricket");
//        SparkConf conf = new SparkConf().setAppName("Twitter Analysis").setMaster("local[2]");
//        JavaSparkContext jsc = new JavaSparkContext(conf);
//        JavaRDD<String> tweetRDD = jsc.parallelize(list);
//        for(String i : tweetRDD.collect())
//                processingTweets(i);
        try{
            FileWriter fw=new FileWriter("/home/krushika/spark-apex-adapter/src/main/resources/data/twitter.txt");
            fw.write(String.valueOf(list));
            fw.close();
        }catch(Exception e){System.out.println(e);}
//        WriteToFS writer = new WriteToFS();
//        writer.write("/twitter/tweets.txt",list);
        ApexConf conf = new ApexConf().setAppName("Twitter Analysis").setMaster("local[3]");
        ApexContext sc = new ApexContext(conf);

        ApexRDD<String> tweetRDD = (ApexRDD<String>) sc.textFile("/home/krushika/spark-apex-adapter/src/main/resources/data/twitter.txt",0);
        for(String i : tweetRDD.collect())
            processingTweets(i);
    }
}
