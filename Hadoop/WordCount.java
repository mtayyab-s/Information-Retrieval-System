/*
 * name: Muhammad Tayyab
 * id: 1001256129
*/
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
    final static String STOP_WORDS_FILE = "stopwords.txt";
    public static class TokenizerMapper 
    extends Mapper<Object, Text, Text, IntWritable>{
        Set<String> stopWords = new HashSet<String>();
       
        @Override
        protected void setup(Mapper.Context context){

            Configuration conf = context.getConfiguration();
            String stp_file_name = conf.get(STOP_WORDS_FILE);
            if(stp_file_name == null)
                return;
            File stp_file = new File(stp_file_name);
            BufferedReader fis;
            try {
                fis = new BufferedReader(new FileReader(stp_file));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                throw new RuntimeException("Could not open stopwords file ",e);
            }
            String word;
            try {
                while((word =fis.readLine()) != null){
                    stopWords.add(word);
                }
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("error while reading stopwords",e);
            }
        }

        
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text filename = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            
             while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if(stopWords.contains(word.toString().toLowerCase())){
                    continue;
                }
                word.toString();
                String words =word+"&"+fileName;
                Text t1 = new Text(words);
                context.write(t1, one);
            }
        }
    }

    public static class IntSumReducer 
    extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, 
                Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    
   

    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> <out> [wordcount stop word file]");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
       // job.setNumReduceTasks(0);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        if(otherArgs.length > 2){
            job.getConfiguration().set(STOP_WORDS_FILE, otherArgs[2]);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}