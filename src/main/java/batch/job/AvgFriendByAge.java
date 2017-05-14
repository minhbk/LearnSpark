package batch.job;

import config.Config;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by minh on 25/01/2017.
 */
public class AvgFriendByAge {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Average Friends By Age");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(Config.DATA_FOLDER_PATH+"fakefriends.csv");
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> pairInput = input.mapToPair(new PairFunction<String, Integer, Tuple2<Integer, Integer>>() {
            public Tuple2<Integer, Tuple2<Integer, Integer>> call(String s) throws Exception {
                List<String> arr = Arrays.asList(s.split(","));
                return new Tuple2<Integer, Tuple2<Integer, Integer>>(
                        Integer.valueOf(arr.get(2)),
                        new Tuple2<Integer, Integer>(Integer.valueOf(arr.get(3)), 1)
                );
            }
        });

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> totalsByAge = pairInput.reduceByKey(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) throws Exception {
                return new Tuple2<Integer, Integer>(t1._1 + t2._1, t1._2 + t2._2);
            }
        });

        JavaPairRDD<Integer, Double> avgsByAge = totalsByAge.mapValues(new Function<Tuple2<Integer, Integer>, Double>() {
            public Double call(Tuple2<Integer, Integer> t) throws Exception {
                return t._1.doubleValue() / t._2;
            }
        });

        JavaPairRDD<Integer, Double> result = avgsByAge.sortByKey();
        for (Tuple2<Integer, Double> t : result.collect()) {
            System.out.println(t._1 + ": " + t._2);
        }
    }
}
