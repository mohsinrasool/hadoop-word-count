/** 
 * Mohsin Rasool 
 * 14-5045
 * Assignment 1
 * Information Retrieval and Text Mining
 * 
 */
package bigdata.wordcount;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new WordCount(), args);
      
      System.exit(res);
   }
   /*
   *  Run function
   *
   */
   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "WordCount");
      job.setJarByClass(WordCount.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   /*
   *  Mapper Class 
   *
   */
   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable ONE = new IntWritable(1);
      private Integer initialLetter=0;

	   /*
	   *  Map function
	   *
	   */
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  
         for (String token: value.toString().split("\\s+")) {
        	 // Skip less than 5 length words
         	if(token.length() < 5 )
         		continue;
			
         	// Fetch first letter of the lengthy word
        	initialLetter = (int) token.toUpperCase().charAt(0);
			
        	// Check if first letter is an alphabet
        	if(initialLetter >=65 && initialLetter <=90) {
        		// Emit the alphabet 
        		context.write( new Text(String.valueOf(token.toUpperCase().charAt(0))) , ONE);
        	}
            
         }
      }
   }
   
   /*
   *  Reducer Class
   */

   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {


	   /*
	   *  Reduce function
	   */
      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
         int sum = 0;
         for (IntWritable val : values) {
            sum += val.get();
         }
         context.write(key, new IntWritable(sum));
      }
   }
}