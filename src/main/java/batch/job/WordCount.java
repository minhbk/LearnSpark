package batch.job;

import config.Config;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by minh on 26/01/2017.
 */
public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(Config.DATA_FOLDER_PATH+"Book.txt");
        JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });


        Map<String, Long> countWords = words.countByValue();

        for (Map.Entry<String, Long> e: countWords.entrySet()){
            System.out.println(e.getKey()+" "+e.getValue());
        }
    }
}
