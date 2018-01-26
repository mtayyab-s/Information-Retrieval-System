
/*
 * name:Muhammad Tayyab
 * id: 1001256129
*/

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class WordCount {
	public static void main(String[] args) {

		// create Spark context with Spark configuration
		JavaSparkContext sc = new JavaSparkContext(
				new SparkConf().setAppName("Spark Count"));

		// read in text file and split each document into words 
		JavaPairRDD<String, String> tk = sc.wholeTextFiles(args[0])
				.flatMapValues(new Function<String, Iterable<String>>() {

					public Iterable<String> call(String fileNameContent)
							throws Exception {
						String content = fileNameContent;

						return Arrays.asList(content.split(" "));

					}
				});

		// filtering the stopwords

		JavaRDD<String> stopWords = sc.textFile(args[1]);
		List<String> stopWordSet = stopWords.collect();

		Function<Tuple2<String, String>, Boolean> longWordFilter = new Function<Tuple2<String, String>, Boolean>() {
			public Boolean call(Tuple2<String, String> keyValue) {
				boolean a = true;
				for (String el : stopWordSet) {

					if (keyValue._2().toLowerCase().equals(el.toString())) {
						a = false;
						break;
					}
				}
				return a;

			}
		};

		
             //The output of result rdd is key value pair where key is the filename and value is the words excluding stopwords
             
             JavaPairRDD<String, String> result = tk.filter(longWordFilter);

		

             
            // counting number of words in  document(n).Its output is a (word$filename,n)

		JavaPairRDD<String, Integer> counts = result.mapToPair(
				new PairFunction<Tuple2<String, String>, String, Integer>() {
					/**
		 * 
		 */
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(Tuple2<String, String> s) {

						return new Tuple2<String, Integer>(s._2 + "&" + s._1, 1);
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			/**
		 * 
		 */
			private static final long serialVersionUID = 1L;

			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		



                JavaPairRDD<String, String> aftercount = counts
				.mapToPair(new PairFunction<Tuple2<String, Integer>, String, String>() {
					/**
    			 * 
    			 */
					private static final long serialVersionUID = 1L;

					public Tuple2<String, String> call(Tuple2<String, Integer> s) {
						String[] k = s._1.split("&");

						return new Tuple2<String, String>(k[1], k[0] + "&"
								+ s._2.toString());
					}
				});

		JavaPairRDD<String, Iterable<String>> wordCountGrouping = aftercount.groupByKey(1);
		
                 
               //totalcount rdd gives sotres result as key vale where key is the filename and value is the total 
                //number of words in that file
               
                JavaPairRDD<String, String> totalcount = wordCountGrouping
                          .mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, String>() {

					public Tuple2<String, String> call(
							Tuple2<String, Iterable<String>> s) {
						Integer sum = 0;
						Iterator<String> iter = s._2.iterator();
						while (iter.hasNext()) {
							String[] b = iter.next().split("&");

							sum = sum + Integer.parseInt(b[1]);
							;
						}

						return new Tuple2<String, String>(s._1, sum.toString());
					}
				});
		
             
     //joined rdd stores key value pair where key is filename and value is N&n&word
           JavaPairRDD<String, Tuple2<String, String>> joined = totalcount.join(aftercount);

		JavaPairRDD<String, String> recompile = joined
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, String>>, String, String>() {
					Integer sum = 0;

					public Tuple2<String, String> call(
							Tuple2<String, Tuple2<String, String>> s) {

						String[] l = s._2._2.split("&");

						return new Tuple2<String, String>(l[0], s._1 + "&"
								+ s._2._1 + "&" + l[1] + "&" + "1");
					}
				});
		JavaPairRDD<String, Iterable<String>> fgroup = recompile.groupByKey(1);

		
               //f_ans stores key value where key is the word and value is m(no. of documents where that word has occured)
                 JavaPairRDD<String, String> f_ans = fgroup
				.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, String>() {

					public Tuple2<String, String> call(
							Tuple2<String, Iterable<String>> s) {
						Integer sum = 0;
						Iterator<String> iter = s._2.iterator();
						while (iter.hasNext()) {
							String[] b = iter.next().split("&");

							sum = sum + Integer.parseInt(b[3]);
							;
						}

						return new Tuple2<String, String>(s._1, sum.toString());
					}
				});

		//joined_final rdd stores key value pair where word is the key and value is filename&N&n,m
         JavaPairRDD<String, Tuple2<String, String>> joined_final = recompile.join(f_ans);

		
            //This gives the result TDIDF value 
        JavaPairRDD<String, Double> final_answer = joined_final
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, String>>, String, Double>() {
					Integer sum = 0;

					public Tuple2<String, Double> call(
							Tuple2<String, Tuple2<String, String>> s) {

						String[] l = s._2._1.split("&");
						Double n = Double.parseDouble(l[2]);
						Double N = Double.parseDouble(l[1]);
						Double m = Double.parseDouble(s._2._2);
						Double answer = (n / N) * Math.log10(10 / m);
						String[] file_name = l[0].split("/");
						String name = file_name[file_name.length - 1];

						return new Tuple2<String, Double>(s._1 + "&" + name,
								answer);
					}
				});

		final_answer.saveAsTextFile(args[2]);
		sc.close();

	}
}
