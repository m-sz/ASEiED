package com.jwszol;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.google.common.base.Optional;

/**
 * Created by kubaw on 29/05/17.
 */
public class JoinJob {

    private static JavaSparkContext sc;

    public JoinJob(JavaSparkContext sc){
        this.sc = sc;
    }

    public static final PairFunction<Tuple2<Integer, Optional<String>>, Integer, String> KEY_VALUE_PAIRER =
            new PairFunction<Tuple2<Integer, Optional<String>>, Integer, String>() {
                public Tuple2<Integer, String> call(
                        Tuple2<Integer, Optional<String>> a) throws Exception {
                    // a._2.isPresent()
                    return new Tuple2<Integer, String>(a._1, a._2.get());
                }
            };


    public static JavaPairRDD<Integer, String> joinData(String t, String u){
        JavaRDD<String> transactionInputFile = sc.textFile(t);

        JavaPairRDD<Integer, Integer> transactionPairs = transactionInputFile.mapToPair(new PairFunction<String, Integer, Integer>() {
            public Tuple2<Integer, Integer> call(String s) {
                String[] transactionSplit = s.split("\t");
                return new Tuple2<Integer, Integer>(Integer.valueOf(transactionSplit[2]), Integer.valueOf(transactionSplit[1]));
            }
        });

        JavaRDD<String> customerInputFile = sc.textFile(u);
        JavaPairRDD<Integer, String> customerPairs = customerInputFile.mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String s) {
                String[] customerSplit = s.split("\t");
                return new Tuple2<Integer, String>(Integer.valueOf(customerSplit[0]), customerSplit[3]);
            }
        });

        JavaRDD<Tuple2<Integer,Optional<String>>> leftJoinOutput = transactionPairs.leftOuterJoin(customerPairs).values().distinct();
        JavaPairRDD<Integer, String> output = leftJoinOutput.mapToPair(KEY_VALUE_PAIRER);

        return output;
    }
}
