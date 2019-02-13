package org.myorg;

/*
 * Rashi Jain
 * rjain12@uncc.edu
 */

import java.io.IOException;
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


public class TFIDF extends Configured implements Tool {

	private static final Logger LOG = Logger .getLogger( TFIDF.class);

	public static void main( String[] args) throws  Exception {
		int res  = ToolRunner .run( new TFIDF(), args);
		System .exit(res);
	}

	public int run( String[] args) throws  Exception {

		// running the first mapreduce job
		Job job1  = Job .getInstance(getConf(), " TermFrequency ");
		job1.setJarByClass( this .getClass());	  
		FileInputFormat.addInputPaths(job1,  args[0]);
		FileOutputFormat.setOutputPath(job1,  new Path(args[ 1])); 
		job1.setMapperClass( TermFrequencyMap .class);
		job1.setReducerClass( TermFrequencyReduce .class);
		job1.setOutputKeyClass( Text .class);
		job1.setOutputValueClass( IntWritable .class);

		// checking for the completion of the job1
		job1.waitForCompletion( true);

		// calculation of the number of documents in the HDFS input folder provided
		Configuration configuration= new Configuration();	
		FileSystem fs = FileSystem.get(configuration);
		ContentSummary cs = fs.getContentSummary(new Path(args[0]));
		long totalDocuments = cs.getFileCount();
		configuration.setInt("numberOfDocuments",(int)totalDocuments);

		// running the second mapreduce job
		System.out.println("-------------------------------------------------Running job 2");
		Job job2  = Job .getInstance(configuration, " TFIDF ");
		job2.setJarByClass( this .getClass());
		FileInputFormat.addInputPaths(job2,  args[1]);
		FileOutputFormat.setOutputPath(job2,  new Path(args[2]));
		job2.setMapperClass( TFIDFMap .class);
		job2.setReducerClass( TFIDFReduce .class);
		job2.setOutputKeyClass( Text .class);
		job2.setOutputValueClass( Text .class);

		// checking for the completion of the job2
		return job2.waitForCompletion( true)  ? 0 : 1;

	}

	/*
	 * Map and Reduce classes for 1st mapReduce job
	 */
	public static class TermFrequencyMap extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
		private final static IntWritable one  = new IntWritable( 1);
		private Text word  = new Text();

		private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {
			String line  = lineText.toString().toLowerCase();
			Text currentWord  = new Text();
			// retrieving the file Name and storing in fileName
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			for ( String word  : WORD_BOUNDARY .split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				/*concatenating word with '#####'and the file Name
				 * so as to set it as the output of the 
				 * map function
				 */
				currentWord  = new Text(word+"#####"+fileName);
				context.write(currentWord,one);
			}
		}
	}

	public static class TermFrequencyReduce extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > {
		@Override 
		public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
				throws IOException,  InterruptedException {
			int sum  = 0;
			double termFrequency; 
			for ( IntWritable count  : counts) {
				sum  += count.get();
			}
			/*
			 * Calculation of term frequency and setting the same
			 * as one of the output parameter of the reduce phase
			 */
			termFrequency = 1 + Math.log10(sum);
			context.write(word,  new DoubleWritable(termFrequency));
		}
	}

	/*
	 * Map and Reduce classes for 2nd mapReduce job
	 */
	public static class TFIDFMap extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

		private Text word  = new Text();

		/*
		 * Input Recieved as: yellow#####file2.txt	1.0
		 * output of mapper : 'yellow' 'file2.txt=1.0'
		 */
		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String wordWithFileName  = lineText.toString();

			String[] wordArray = wordWithFileName.split("#####"); 

			// replacing tabs with = 
			String value = wordArray[1].replaceAll("\\s+","=");
			Text currentWord  = new Text(wordArray[0]);
			Text currentWordValue = new Text(value);
			context.write(currentWord, currentWordValue);
		}
	}


	/*
	 * Input Recieved as: 'Hadoop' [file1.txt=0.3010299956639813, file2.txt=1.0]
	 * output of reducer : Hadoop#####file1.txt	0.30102999566
	 * 					   Hadoop#####file2.txt 0.39164905394	
	 */
	public static class TFIDFReduce extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
		@Override 
		public void reduce( Text word,  Iterable<Text > counts,  Context context)
				throws IOException,  InterruptedException {

			int docFrequency = 0;
			HashMap<String, String> intermediateValues = new HashMap<>();
			for(Text count: counts) {
				String [] temp_array = count.toString().split("=");
				/*
				 * temp_array[0] contains file1.txt
				 * temp_array[1] contains the term frequency
				 */
				intermediateValues.put(temp_array[0],temp_array[1]);
				docFrequency+=1;
			}
			// fetching the total number of documents from the context.getConfiguration()
			int totalDocuments =Integer.parseInt(context.getConfiguration().get("numberOfDocuments")); 

			// calculation of IDF
			double IDF = Math.log10(1 + (double)totalDocuments/(double)docFrequency);
			System.out.println("********************************"+IDF);

			double TFIDF;

			// calculating TFIDF for the word with reference to the values stored in intermediateValues HashMap
			for(Map.Entry<String,String> hmap : intermediateValues.entrySet()) {
				TFIDF = IDF * Double.parseDouble(hmap.getValue());
				System.out.println("***************************"+TFIDF);
				
				/*concatenating word with '#####'and the file Name which is the key of the HashMap
				 * so as to set it as the output of the reduce function
				 */
				Text currentWord  = new Text(word + "#####" +hmap.getKey());
				context.write(currentWord,  new DoubleWritable(TFIDF));
			}	

		}
	}  
}
