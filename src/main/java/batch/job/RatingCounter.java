package batch.job;

import config.Config;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import java.util.Arrays;
import java.util.Map;

/**
 * Created by minh on 24/01/2017.
 */
public class RatingCounter {

    public static void main(String[] args){

        SparkConf conf = new SparkConf().setMaster("local").setAppName("ratings counter");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.
                textFile(Config.DATA_FOLDER_PATH+"ml-100k/u.data");
        JavaRDD<Integer> ratings = input.map(new Function<String, Integer>() {
            public Integer call(String s) throws Exception {
                return Integer.valueOf(Arrays.asList(s.split("\\s")).get(2));
            }
        });

//        JavaPairRDD<Integer, Integer> result = ratings.mapToPair(new PairFunction<Integer, Integer, Integer>() {
//            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
//                return new Tuple2<Integer, Integer>(integer, 1);
//            }
//        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//            public Integer call(Integer integer, Integer integer2) throws Exception {
//                return integer + integer2;
//            }
//        }).sortByKey();


//        for (Tuple2<Integer, Integer> t: result.collect()){
//            System.out.println(t._1+" "+t._2);
//        }

        //This way can't sort key!
        Map<Integer, Long> result = ratings.countByValue();

        for (Map.Entry<Integer, Long> e: result.entrySet()){
            System.out.println(e.getKey()+" "+e.getValue());
        }

    }


}
