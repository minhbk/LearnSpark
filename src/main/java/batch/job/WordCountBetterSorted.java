package batch.job;

import config.Config;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by minh on 27/01/2017.
 */
public class WordCountBetterSorted {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(Config.DATA_FOLDER_PATH+"Book.txt");
        JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                String lowerStr = s.toLowerCase();
                return Arrays.asList(lowerStr.split("\\W+")).iterator();
            }
        });

        JavaPairRDD<String, Integer> countWords = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaPairRDD<Integer, String> countWordsSorted = countWords.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                return new Tuple2<Integer, String>(tuple._2, tuple._1);
            }
        }).sortByKey();

        for (Tuple2<Integer, String> t: countWordsSorted.collect()){
            System.out.println(t._2+" "+t._1);
        }

    }
}
