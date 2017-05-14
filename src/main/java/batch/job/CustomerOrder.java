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
 * Created by minh on 29/01/2017.
 */
public class CustomerOrder {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Customer Order");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(Config.DATA_FOLDER_PATH+"customer-orders.csv");

        JavaPairRDD<Integer, Double> orders = input.mapToPair(new PairFunction<String, Integer, Double>() {
            public Tuple2<Integer, Double> call(String s) throws Exception {
                List<String> arr = Arrays.asList(s.split(","));
                return new Tuple2<Integer, Double>(Integer.valueOf(arr.get(0)), Double.valueOf(arr.get(2)));
            }
        });

        JavaPairRDD<Integer, Double> totalOrders = orders.reduceByKey(new Function2<Double, Double, Double>() {
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return aDouble + aDouble2;
            }
        }).sortByKey();

        for (Tuple2<Integer, Double> t: totalOrders.collect()){
            System.out.println(t._1+" "+t._2);
        }
    }
}
