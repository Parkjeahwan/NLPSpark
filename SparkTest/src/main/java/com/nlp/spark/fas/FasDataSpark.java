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
public class FasDataSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("fas data extraction App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(args[0]);

        JavaRDD<String> fasbackdata = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                String infos[] = s.split("\t");
                char word[] = infos[0].toCharArray();

                List<String> list = new ArrayList<>();

                int u = 10;
                for (int k0 = 1; k0 <= u; k0++) {
                    HashSet<String> checkm1 = new HashSet<>();
                    for (int i = 0; i < word.length-k0+1; i++) {
                        String m1 = "";
                        for (int ii = i; ii < i+k0; ii++) m1 += word[ii];

                        String checkStr = m1 + "\t" + infos[1] + "/" + i;

                        if (checkm1.contains(checkStr)) continue;
                        else checkm1.add(m1);

                        list.add(checkStr);
                    }
                }
                return list;
            }
        });

        JavaPairRDD<String, String> pairs = fasbackdata.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>(s.split("\t")[0], s);
            }
        });

        JavaPairRDD<String, String> merge = pairs.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1 + "," + v2;
            }
        });

        merge.saveAsTextFile(args[1]);
    }
}
