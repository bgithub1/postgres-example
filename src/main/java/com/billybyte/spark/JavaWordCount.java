package com.billybyte.spark;
//com.billybyte.spark.JavaWordCount
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public final class JavaWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {
	Calendar in = Calendar.getInstance();
    if (args.length < 1) {
      System.err.println("Usage: JavaWordCount <file>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
//    JavaRDD<String> lines = ctx.textFile(args[0], 1);
//
//    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//      @Override
//      public Iterable<String> call(String s) {
//        return Arrays.asList(SPACE.split(s));
//      }
//    });
//
//    JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
//      @Override
//      public Tuple2<String, Integer> call(String s) {
//        return new Tuple2<String, Integer>(s, 1);
//      }
//    });
//
//    JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
//      @Override
//      public Integer call(Integer i1, Integer i2) {
//        return i1 + i2;
//      }
//    });
//
//    List<Tuple2<String, Integer>> output = counts.collect();
//    for (Tuple2<?,?> tuple : output) {
//      System.out.println(tuple._1() + ": " + tuple._2());
//    }
//    
//    
    SQLContext sqlContext = new SQLContext(ctx);
    String[] etfs = {
    		"XLB","XLE","XLF","XLK","XLI","XLP","XLU","XLV","XLY"
    };
    String[] etfPaths = new String[etfs.length];
    String pathStart = "/Users/bperlman1/Dropbox/prshare/rStuff/data/etfs1min/convertedUnAdjusted/";
    String pathEnd = ".csv";
    for(int i = 0;i< etfs.length;i++){
    	etfPaths[i] = pathStart+etfs[i]+pathEnd;
    }
    HashMap<String, String> options = new HashMap<String, String>();
    options.put("header", "true");
    List<DataFrame> dfList = new ArrayList<>();
    Map<String, String> m = new HashMap<>();
    m.put("close", "sum");
    for(int i = 0;i< etfs.length;i++){
    	options.put("path", etfPaths[i]);	
        DataFrame df = sqlContext.load("com.databricks.spark.csv", options);
        df.agg(m).show();
    }
    
    

//    DataFrame df = sqlContext.load("com.databricks.spark.csv", options);
//    df.show();
//    DataFrame dfClEq100 = df.filter(df.col("Close").equalTo(100.00));
//    dfClEq100.show();
//    Map<String, String> m = new HashMap<>();
//    m.put("Close", "sum");
//    dfClEq100.agg(m).show();
    ctx.stop();
    ctx.close();
    
    long elapsed = Calendar.getInstance().getTimeInMillis() - in.getTimeInMillis();
    System.out.println("elapsed = "+elapsed);
  }
}