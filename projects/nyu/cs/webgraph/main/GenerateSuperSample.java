package nyu.cs.webgraph.main;

import java.io.IOException;
import java.util.Random;

import nyu.cs.webgraph.MRhelpers.CustomFileInputFormats.TabSeperatedTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GenerateSuperSample {
	
	// set sampling frequency here!
	public static final int desiredNumOfSamples = 10000000;
	public static final String NUM_OF_INPUT_RECORDS = "numOfInputRecord";
	public static final String IS_KEY_ONLY = "iskeyonly";
	
	public static Text emptyText = new Text("");
	
	public static class RandomPickMapper extends Mapper<Text, Text, Text, Text> {
		double freq; 
		boolean iskeyonly;
		public void setup(Context context){
			
			int numOfrecords = Integer.parseInt(context.getConfiguration().get(NUM_OF_INPUT_RECORDS));
			if (numOfrecords <= desiredNumOfSamples ) 
				freq = 1;
			else    
				freq = ((double)desiredNumOfSamples / (double)numOfrecords);
			
			iskeyonly = Boolean.parseBoolean(context.getConfiguration().get(IS_KEY_ONLY));
		}
		
		Text src = new Text();
		Random rndm = new Random();
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			
			long seed = rndm.nextLong();
			rndm.setSeed(seed);
			// sample source only
			if (rndm.nextDouble() <= freq) {
				if (iskeyonly)
					context.write(key, emptyText);
				else
					context.write(value, emptyText);
			}
		}
	}
	
	public static class IdenityReducer extends Reducer<Text, Text, Text, Text> {
		
		Integer counter = 0;
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			counter = 0;
		}
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values){
				counter++;
				context.write(key, val);
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			FileSystem fs = FileSystem.get(new Configuration());
	        
			//writing 
	        FSDataOutputStream dos = fs.create(new Path(makeSSampleCounterPathName(FileOutputFormat.getOutputPath(context).toString())), true); 
	        dos.writeInt(counter); 
	        dos.close(); 
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		String inpath = "", outpath = "";
		Boolean isKeyOnly = true;
		Integer numOfInRecords = 0; 
		for(int i=0; i < args.length; ++i) {
		      try {
		        if ("-inpath".equals(args[i])) {
		        	i++;
		        	inpath += args[i]+",";
		        }  else if ("-outpath".equals(args[i])) {
		        	i++;
		        	outpath = args[i];
		        } else if ("-numrecords".equals(args[i])) {
		        	i++;
		        	numOfInRecords = Integer.parseInt(args[i]);
		        } else if ("-keyonly".equals(args[i])) {
		        	i++;
		        	isKeyOnly = Boolean.parseBoolean(args[i]);
		        } else {
		          throw new NullPointerException(args[i]);
		        }
		      } catch (NullPointerException except) {
		        System.out.println("ERROR: inappropiate argument " + args[i]);
		        return;
		      } catch (ArrayIndexOutOfBoundsException except) {
		        System.out.println("ERROR: Required parameter missing from " + args[i-1]);
		        System.out.println("usage:  -inpath inputPath -outpath outputPath");
		        return; // exits
		      }
		}
		
		System.out.println("sampling keys only?" + isKeyOnly);
		
		Configuration conf = new Configuration();
		conf.set(NUM_OF_INPUT_RECORDS, numOfInRecords.toString());
		conf.set(IS_KEY_ONLY, isKeyOnly.toString());
		
		Job job = Job.getInstance(new Cluster(conf), conf);
		job.setJobName("generate super sample");
		job.setJarByClass(GenerateSuperSample.class);
		
		FileInputFormat.setInputPaths(job, inpath.substring(0, inpath.length()-1));
		FileOutputFormat.setOutputPath(job, new Path(outpath));
		
		job.setInputFormatClass(TabSeperatedTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(RandomPickMapper.class);
		job.setReducerClass(IdenityReducer.class);
		job.setNumReduceTasks(1);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		TextOutputFormat.setCompressOutput(job, true);
		TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		job.waitForCompletion(true);
	}
	
	public static String makeSSampleCounterPathName(String ssamplePathName){
		return ssamplePathName + "-counter";
	}
	
}
