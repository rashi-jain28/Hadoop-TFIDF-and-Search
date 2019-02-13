package org.myorg;

/*
 * Rashi Jain
 * rjain12@uncc.edu
 */

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import java.util.*;


public class Search extends Configured implements Tool {

	private static final Logger LOG = Logger .getLogger( Search.class);

	public static void main( String[] args) throws  Exception {
		int res  = ToolRunner .run( new Search(), args);
		System .exit(res);
	}

	public int run( String[] args) throws  Exception {

		// running the mapReduce job 
		Job job1  = Job .getInstance(getConf(), " Search ");
		job1.setJarByClass( this .getClass());	  
		FileInputFormat.addInputPaths(job1,  args[0]);
		FileOutputFormat.setOutputPath(job1,  new Path(args[ 1])); 
		job1.setMapperClass( Map .class);
		job1.setReducerClass( Reduce .class);
		job1.setOutputKeyClass( Text .class);
		job1.setOutputValueClass( DoubleWritable .class);

		/* setting the query passed by the user from the command to the Configuration
		 * using arg[2] as the query parameter "yellow Hadoop" 
		 */
		Configuration configuration = job1.getConfiguration();
		configuration.set("SearchQuery",args[2]);

		return job1.waitForCompletion( true)  ? 0 : 1;

	}

	/*
	 * Input : Hadoop#####file1.txt	0.30102999566
	 * 		  Hadoop#####file2.txt 0.39164905394	
	 * 
	 * Query: yellow Hadoop
	 * 
	 * Output :  file1.txt 0.30102999566
	 * 			 file2.txt 0.39164905394
	 */
	public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {

		private Text word  = new Text();

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			/* extracting the query given by the user
			 * query received = yellow Hadoop
			 */
			String query = context.getConfiguration().get("SearchQuery");
			
			// splitting the query based on the blank spaces
			String[] tokens = query.toLowerCase().split("\\s+");
			
			// splitting the input based on the space
			String[] s = lineText.toString().split("\\s+");

			String tfidfValue = s[1];
			String[] word = s[0].split("#####");

			/*
			 * NOTE: word[0] is each word
			 * 		 word[1] is fileName
			 * 		 s[1] contains the tfidf values
			 */
			for(String value: tokens){
				if(value.equalsIgnoreCase(word[0])){
					context.write(new Text(word[1]),new DoubleWritable(Double.valueOf(tfidfValue)));
				}	
			}
		}
	}
	
	/*
	 * Input :  file1.txt,[ 0.30102999566, 0.39164905394]
	 * Output : file1.txt 0.6926790496
	 */
	public static class Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
		@Override 
		public void reduce( Text word,  Iterable<DoubleWritable > counts,  Context context)
				throws IOException,  InterruptedException {
			double sum  = 0;
			for ( DoubleWritable count  : counts) {
				sum  += count.get();
			}
			context.write(word,  new DoubleWritable(sum));
		}
	}



}


