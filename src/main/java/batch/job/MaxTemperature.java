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
import scala.Tuple3;

import java.util.Arrays;
import java.util.List;

/**
 * Created by minh on 26/01/2017.
 */
public class MaxTemperature {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Minimum Temperatures");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(Config.DATA_FOLDER_PATH+"1800.csv");
        JavaRDD<Tuple3<String, String, Double>> parsedLines = input.map(new Function<String, Tuple3<String, String, Double>>() {
            public Tuple3<String, String, Double> call(String s) throws Exception {
                List<String> arr = Arrays.asList(s.split(","));
                String stationID = new String(arr.get(0));
                String entryType = new String(arr.get(2));
                Double temperature = Double.parseDouble(arr.get(3)) * 0.1 * (9.0 / 5.0) + 32.0;
                return new Tuple3<String, String, Double>(stationID, entryType, temperature);
            }
        });

        JavaRDD<Tuple3<String, String, Double>> filterLines = parsedLines.filter(new Function<Tuple3<String, String, Double>, Boolean>() {
            public Boolean call(Tuple3<String, String, Double> tuple3) throws Exception {
                return tuple3._2().equals("TMAX");
            }
        });

        JavaPairRDD<String, Double> stationTemps = filterLines.mapToPair(new PairFunction<Tuple3<String, String, Double>, String, Double>() {
            public Tuple2<String, Double> call(Tuple3<String, String, Double> tuple3) throws Exception {
                return new Tuple2<String, Double>(tuple3._1(), tuple3._3());
            }
        });

        JavaPairRDD<String, Double> minTemps = stationTemps.reduceByKey(new Function2<Double, Double, Double>() {
            public Double call(Double d1, Double d2) throws Exception {
                return Math.max(d1, d2);
            }
        });

        for (Tuple2<String, Double> t: minTemps.collect()){
            System.out.println(t._1+" "+ t._2);
        }
    }
}
