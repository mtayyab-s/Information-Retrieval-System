/*
 * name: Muhammad Tayyab
 * id: 1001256129

*/
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Result {
	public static class FourthMapper
    extends Mapper<Object, Text, Text,Text>{
	 private final static IntWritable one = new IntWritable(1);
	 private Text test = new Text();
	 public void map(Object key, Text value, Context context
		        ) throws IOException, InterruptedException {
		
		 StringTokenizer itrat3 = new StringTokenizer(value.toString(),"\n");
		 
       while (itrat3.hasMoreTokens()) {
            test.set(itrat3.nextToken());
            String k =value.toString().replace('\t','&');
			 String[] result = k.split("&");
			 String output_key=result[0]+"&"+result[1];
			 double n=Double.parseDouble(result[2]);
			 double N=Double.parseDouble(result[3]);
			 double m=Double.parseDouble(result[4]);
			 String TDIDF = Double.toString((n/N)*(Math.log10(10/m)));
            context.write(new Text(output_key),new Text(TDIDF));
         }
	 }
	}
 public static void main(String[] args) throws Exception {
		 
		 
		 Configuration conf5 = new Configuration();
	     String[] otherArgs = new GenericOptionsParser(conf5, args).getRemainingArgs();
	       Job jobs = new Job(conf5, "Result");
	        jobs.setJarByClass(Result.class);
	        jobs.setMapperClass(FourthMapper.class);
	        jobs.setNumReduceTasks(0);
	        jobs.setOutputKeyClass(Text.class);
	        jobs.setOutputValueClass(Text.class);
	        FileInputFormat.addInputPath(jobs, new Path(otherArgs[0]));
	        FileOutputFormat.setOutputPath(jobs, new Path(otherArgs[1]));
	        
	        System.exit(jobs.waitForCompletion(true) ? 0 : 1);
	 }
}



