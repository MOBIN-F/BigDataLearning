//package com.mobin.SparkRDDFun.TransFormation.KVRDD;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.SparkContext;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.broadcast.Broadcast;
//import scala.Tuple2;
//
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//
///**
// * Created by Mobin on 2016/11/14.
// */
//public class MapJoinJava {
//    public static void main(String[] args) {
//        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("JavaMapSide");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        JavaRDD<String> table =  sc.textFile("mapjoin.txt");
//        JavaRDD<String> table1 =  sc.textFile("mapjoin1.txt");
//
//        final Map<String, String> pairs = table.mapToPair(new PairFunction<String, String, String>() {
//            public Tuple2<String, String> call(String s) throws Exception {
//                int pos = s.indexOf(",");
//                return new Tuple2<String, String>(s.substring(0,pos), s.substring(pos + 1));
//            }
//        }).collectAsMap();
//
//       // final Broadcast<Map<String,String>> broadcast = sc.broadcast(pairs);
//
//        table1.mapToPair(new PairFunction<String, String, String>() {
//            public Tuple2<String, String> call(String s) throws Exception {
//                int pos = s.indexOf(",");
//                return new Tuple2<String, String>(s.substring(0,pos), s.substring(pos + 1));
//            }
//        }).mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,String>>, Tuple2<String,List<String>>>() {
//            public Iterable<Tuple2<String, List<String>>> call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
//                List<Tuple2<String, List<String>>> list = null;
//                List l = new ArrayList();
//                while (tuple2Iterator.hasNext()){
//                    Tuple2<String,String> map = tuple2Iterator.next();
//                    if (pairs.containsKey(map._1)){
//                        if(list == null)
//                            list = new ArrayList();
//
//                            l.add(pairs.get(map._1));
//                            l.add(map._2);
//                            list.add(new Tuple2<String, List<String>>(map._1,l));
//                    }
//                }
//                return list;
//            }
//        }).saveAsTextFile("javaMapJoin");
//    }
//
//
//}
