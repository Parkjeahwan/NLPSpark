package com.nlp.spark.fas;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by parkjh on 16. 8. 31.
 */
public class NPairDataSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("N pair data out App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(args[0]);
        JavaRDD<String> npair = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                char str[] = s.toCharArray();
                List<String> list = new ArrayList<>();

                int u = 10;
                for (int k0 = 1; k0 <= u; k0++) {
                    HashSet <String> checkm1 = new HashSet <> ();
                    for (int i = 0; i < str.length - k0 + 1; i++) {
                        HashSet<String> checkm2 = new HashSet <> ();
                        String m1 = "";
                        for (int ii = i; ii < i + k0; ii++) m1 += str[ii];

                        if (checkm1.contains(m1)) continue;
                        else checkm1.add(m1);

                        for (int k1 = 1; k1 <= u; k1++) {
                            String m2 = "";
                            for (int j = i + k0; i < str.length - k0 - k1 + 1 && j < i + k0 + k1; j++) m2 += str[j];
                            if(m2.equals("")) continue;

                            if (m1.equals(m2) || checkm2.contains(m2)) continue;
                            else checkm2.add(m2);

                            String keys = m1 + " ##SP " + m2;
                            list.add(keys);
                        }
                    }
                }
                return list;
            }
        });

        JavaPairRDD<String, Integer> pairs = npair.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        counts.saveAsTextFile(args[1]);
    }
}
