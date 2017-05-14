package batch.job;

import config.Config;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Created by minh on 12/02/2017.
 */
public class MostPopularSuperHero {


    public static void main(String[] args) {


        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Most popular super hero");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> input = jsc.textFile(Config.DATA_FOLDER_PATH+"Marvel-Graph.txt");
        JavaRDD<String> name = jsc.textFile(Config.DATA_FOLDER_PATH+"Marvel-Names.txt");

//        JavaPairRDD<Integer, String> nameRdd = name.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, Integer, String>() {
//            public Iterator<Tuple2<Integer, String>> call(Iterator<String> stringIterator) throws Exception {
//                List<Tuple2<Integer, String>> result = new ArrayList<Tuple2<Integer, String>>();
//                List<String> split;
//                String str;
//                while (stringIterator.hasNext()){
//                    str = stringIterator.next();
//                    split = Arrays.asList(str.split("\""));
//                    result.add(new Tuple2<Integer, String>(Integer.valueOf(split.get(0).trim()), split.get(1)));
//                }
//                return result.iterator();
//            }
//        });

        JavaPairRDD<Integer, Integer> parse = input.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, Integer, Integer>() {
            public Iterator<Tuple2<Integer, Integer>> call(Iterator<String> stringIterator) throws Exception {
                List<Tuple2<Integer, Integer>> result = new ArrayList<Tuple2<Integer, Integer>>();
                List<String> split;
                String str;
                while (stringIterator.hasNext()) {
                    str = stringIterator.next();
                    split = Arrays.asList(str.split(""));
                    result.add(new Tuple2<Integer, Integer>(Integer.valueOf(split.get(0)), split.size() - 1));
                }
                return result.iterator();
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaPairRDD<Integer, Integer> flipped = parse.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer, Integer>>, Integer, Integer>() {
            public Iterator<Tuple2<Integer, Integer>> call(Iterator<Tuple2<Integer, Integer>> tuple2Iterator) throws Exception {
                List<Tuple2<Integer, Integer>> result = new ArrayList<Tuple2<Integer, Integer>>();
                Tuple2<Integer, Integer> tuple;

                while (tuple2Iterator.hasNext()) {
                    tuple = tuple2Iterator.next();
                    result.add(new Tuple2<Integer, Integer>(tuple._2(), tuple._1()));
                }
                return result.iterator();
            }
        });

        JavaPairRDD<Integer, Integer> sorter = flipped.sortByKey(false);
        Tuple2<Integer, Integer> max = sorter.first();

//        Tuple2<Integer, Integer> max = flipped.max(new Comparator<Tuple2<Integer, Integer>>() {
//            public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
//                if (o1._1() > o2._1()){
//                    return 1;
//                } else if (o1._1() < o2._1()){
//                    return -1;
//                } else {
//                    return 0;
//                }
//            }
//        });
//        String nameMax = nameRdd.lookup(max._2()).get(0);

        System.out.println(max._2()+" "+max._1());
    }

}
