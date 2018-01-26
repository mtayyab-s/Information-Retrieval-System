/*
 * name: Muhammad Tayyab	
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class ThirdJob {
	
	public static class ThirdMapper
    extends Mapper<Object, Text, Text, Text>{
	 private final static IntWritable one = new IntWritable(1);
	 private Text test = new Text();
	 public void map(Object key, Text value, Context context
		        ) throws IOException, InterruptedException {
		
		 StringTokenizer itrat2 = new StringTokenizer(value.toString(),"\n");
		 
       while (itrat2.hasMoreTokens()) {
            test.set(itrat2.nextToken());
            String k =value.toString().replace('\t','&');
			 String[] result = k.split("&");
			 Text word=new Text(result[0]);
			 Text outp = new Text(result[1]+"&"+result[2]+"&"+result[3]+"&"+"1");
            context.write(word,outp);
         }
	 }
	}
	
	 public static class DocumentCountReducers 
	    extends Reducer<Text,Text,Text,Text> {
	       ArrayList<String> fileList = new ArrayList<String>();
	       ArrayList<String> outputList = new ArrayList<String>();
	        public void reduce(Text key, Iterable<Text> values, 
	                Context context4
	        ) throws IOException, InterruptedException {
	        	fileList.clear();
	        	outputList.clear();
	        	int sum = 0;
	            for (Text val : values) {
	            	String[] result2=val.toString().split("&");
	            	int count=  Integer.parseInt(result2[3]);
	            	sum+=count;
	            	fileList.add(result2[0]);
	                outputList.add(result2[1]+"&"+result2[2]);
	            }
	            
	          String c=Integer.toString(sum);
	            for(int i=0;i<fileList.size();i++){
	            context4.write(new Text(key.toString()+"&"+fileList.get(i)),new Text(outputList.get(i)+"&"+c));
	            }
	        }
	    }	
	
	
	
 public static void main(String[] args) throws Exception {
		 
		 
		 Configuration conf3 = new Configuration();
	     String[] otherArgs = new GenericOptionsParser(conf3, args).getRemainingArgs();
	       Job jobs2 = new Job(conf3, "file count");
	        jobs2.setJarByClass(ThirdJob.class);
	        jobs2.setMapperClass(ThirdMapper.class);
	        jobs2.setReducerClass(DocumentCountReducers .class);
	        jobs2.setOutputKeyClass(Text.class);
	        jobs2.setOutputValueClass(Text.class);
	        FileInputFormat.addInputPath(jobs2, new Path(otherArgs[0]));
	        FileOutputFormat.setOutputPath(jobs2, new Path(otherArgs[1]));
	        
	        System.exit(jobs2.waitForCompletion(true) ? 0 : 1);
	 }
	
	
	
}
 
 

