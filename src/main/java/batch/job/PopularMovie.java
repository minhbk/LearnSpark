package batch.job;

import config.Config;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by minh on 30/01/2017.
 */
public class PopularMovie {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Popular Movie");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(Config.DATA_FOLDER_PATH+"ml-100k/u.data");
        JavaPairRDD<Integer, Integer> movies = input.mapToPair(new PairFunction<String, Integer, Integer>() {
            public Tuple2<Integer, Integer> call(String s) throws Exception {
                List<String> arr = Arrays.asList(s.split("\\s"));
                return new Tuple2<Integer, Integer>(Integer.valueOf(arr.get(1)), 1);
            }
        });

        JavaPairRDD<Integer, Integer> movieCounts = movies.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaPairRDD<Integer, Integer> flipped = movieCounts.mapToPair(new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> tuple2) throws Exception {
                return new Tuple2<Integer, Integer>(tuple2._2, tuple2._1);
            }
        }).sortByKey();

        for (Tuple2<Integer, Integer> t: flipped.collect()){
            System.out.println(t._1+" "+t._2);
        }
    }
}
