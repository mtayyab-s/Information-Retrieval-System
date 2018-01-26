/*
 * name:Muhammad Tayyab
 * id: 1001256129
*/

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class WordsPerDocument {
	 public static class FileMapper 
	    extends Mapper<Object, Text, Text, Text>{
		 private final static IntWritable one = new IntWritable(1);
		 private Text test = new Text();
		 public void map(Object key, Text value, Context context
			        ) throws IOException, InterruptedException {
			
			 StringTokenizer itrat = new StringTokenizer(value.toString(),"\n");
			 
           while (itrat.hasMoreTokens()) {
                test.set(itrat.nextToken());
                String k =value.toString().replace('\t','&');
   			 String[] result = k.split("&");
   			 Text r = new Text( result[1]);
   			 Text m = new Text(result[0]+"&"+result[2]);
                context.write(r,m);
             }
			 
			 
		 }
	 }
	 
	 
	 public static class IntSumReducers 
	    extends Reducer<Text,Text,Text,Text> {
	       private IntWritable result = new IntWritable();
	       ArrayList<String> list = new ArrayList<String>();
	       ArrayList<String> list2 = new ArrayList<String>();
	        public void reduce(Text key, Iterable<Text> values, 
	                Context context
	                
	        ) throws IOException, InterruptedException {
	        	list.clear();
	        	list2.clear();
	            int sum = 0;
	            for (Text val : values) {
	            	String[] result2=val.toString().split("&");
	            	int count=  Integer.parseInt(result2[1]);
	            	sum+=count;
	            	list.add(result2[1]);
	               String a =result2[0];
	               list2.add(a);
	            }
	            
	           String c=Integer.toString(sum);
	            for(int i=0;i<list.size();i++){
	            context.write(new Text(list2.get(i)+"&"+key.toString()),new Text(list.get(i)+"&"+c));
	            }
	        }
	    }
	 public static void main(String[] args) throws Exception {
		 
		 
		 Configuration conf = new Configuration();
	     String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	       Job jobs = new Job(conf, "words per document");
	        jobs.setJarByClass(WordsPerDocument.class);
	        jobs.setMapperClass(FileMapper.class);
	       jobs.setReducerClass(IntSumReducers.class);
	        jobs.setOutputKeyClass(Text.class);
	        jobs.setOutputValueClass(Text.class);
	        FileInputFormat.addInputPath(jobs, new Path(otherArgs[0]));
	        FileOutputFormat.setOutputPath(jobs, new Path(otherArgs[1]));
	        
	        System.exit(jobs.waitForCompletion(true) ? 0 : 1);
	 }
}
