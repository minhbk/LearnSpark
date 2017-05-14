package batch.job;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import config.Config;

import java.util.Arrays;
import java.util.List;

/**
 * Created by minh on 30/01/2017.
 */
public class CustomerOrderSortedAmountSpent {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Customer Order sort the final results by amount spent");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> input = sc.textFile(Config.DATA_FOLDER_PATH+"customer-orders.csv");

        JavaPairRDD<Integer, Double> orders = input.mapToPair(new PairFunction<String, Integer, Double>() {
            public Tuple2<Integer, Double> call(String s) throws Exception {
                List<String> arr = Arrays.asList(s.split(","));
                return new Tuple2<Integer, Double>(Integer.valueOf(arr.get(0)), Double.valueOf(arr.get(2)));
            }
        });

        JavaPairRDD<Double, Integer> totalOrders = orders.reduceByKey(new Function2<Double, Double, Double>() {
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return aDouble + aDouble2;
            }
        }).mapToPair(new PairFunction<Tuple2<Integer, Double>, Double, Integer>() {
            public Tuple2<Double, Integer> call(Tuple2<Integer, Double> tuple2) throws Exception {
                return new Tuple2<Double, Integer>(tuple2._2, tuple2._1);
            }
        }).sortByKey();

        for (Tuple2<Double, Integer> t : totalOrders.collect()) {
            System.out.println(t._2 + " " + t._1);
        }
    }
}
