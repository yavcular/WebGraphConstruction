package nyu.cs.webgraph.updates;

import java.io.IOException;

import nyu.cs.webgraph.MRhelpers.CustomFileInputFormats.TabSeperatedTextInputFormat;
import nyu.cs.webgraph.main.Sample;
import nyu.cs.webgraph.main.GetUniqueUrls.GetUniqueUrlsReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

/*
 * Map file format
 * id str
 * id str
 */

public class MergeUrlIdMaps {
	
	public static final int numOfReducers = 144;
	public static final String CURRENT_MAP_IDENTIFIER = "c_", NEW_MAP_IDENTIFIER = "n_";
	public static final String IS_KEY_FIRST_CUREENT_MAP = "isKeyFirstCurrent", IS_KEY_FIRST_NEW_MAP = "isKeyFirstNew";
	
	public static String appendCurrentMappingIdentifier(String id){
		return CURRENT_MAP_IDENTIFIER + id;
	}
	public static String appendNewMappingIdentifier(String id){
		return NEW_MAP_IDENTIFIER + id;
	}
	public static String removeCurrentMapIdentifier(String str){
		return str.substring(CURRENT_MAP_IDENTIFIER.length());
	}
	public static String removeNewMapIdentifier(String str){
		return str.substring(NEW_MAP_IDENTIFIER.length());
	}
	
	public static class GenerateCurrentMapTuples extends Mapper<Text, Text, Text, Text> {
		
		Text val = new Text();
		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			if(Boolean.parseBoolean(context.getConfiguration().get(IS_KEY_FIRST_CUREENT_MAP))){
				val.set(appendCurrentMappingIdentifier(value.toString()));
				context.write(key, val);
			}
			else{
				val.set(appendCurrentMappingIdentifier(key.toString()));
				context.write(value, val);
			}
		}
	}
	public static class GenerateNewMapTuples extends Mapper<Text, Text, Text, Text> {
		
		Text val = new Text();
		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			if(Boolean.parseBoolean(context.getConfiguration().get(IS_KEY_FIRST_NEW_MAP))){
				val.set(appendNewMappingIdentifier(value.toString()));
				context.write(key, val);
			}
			else{
				val.set(appendNewMappingIdentifier(key.toString()));
				context.write(value, val);
			}
		}
	}
	
	//extends uurlReducer for counter
	public static class MergeMappingTuplesReducer extends GetUniqueUrlsReducer{
		StringBuffer value;
		Text initial_mappings = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			counter += 1;
			if (isFirst)
			{
				firtUrl.set(key);
				isFirst = false;
			}
			
			value = new StringBuffer();
			for (Text text : values) {
				value.append(text.toString()).append(" ");
			}
			initial_mappings.set(value.toString());
			context.write(key, initial_mappings);
		}
	}
	
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		System.out.println("merging url maps");
		
		if (args.length != 7)
			throw new IllegalArgumentException("Incorrect number of parameters: <current_map_path>, <is_key_first_current>, <new_map_path>, <is_key_first_new>, <merged_map_path> <supersample_path> <sample_path>");
		
		String current_map_path  = args[0], is_key_first_current = args[1], new_map_path = args[2], is_key_first_new = args[3], final_map_path  = args[4];
		String supersample_path = args[5], sample_path = args[6];
		
		//first get the sample file for partitioner using supersample
		Sample.sampleForTotalOrderPatitioner(supersample_path, sample_path, numOfReducers);
		System.out.println("Sample for partitioner is writen.");
		
		Configuration conf = new Configuration();
		conf.set(IS_KEY_FIRST_CUREENT_MAP, is_key_first_current);
		conf.set(IS_KEY_FIRST_NEW_MAP, is_key_first_new);
		TotalOrderPartitioner.setPartitionFile(conf, new Path(sample_path));
		
		Job job = Job.getInstance(new Cluster(conf), conf);
		job.setJobName("update: merge maps");
		job.setJarByClass(MergeMappingTuplesReducer.class);
		
		MultipleInputs.addInputPath(job, new Path(current_map_path), TabSeperatedTextInputFormat.class,GenerateCurrentMapTuples.class);
		MultipleInputs.addInputPath(job, new Path(new_map_path), TabSeperatedTextInputFormat.class, GenerateNewMapTuples.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		job.setReducerClass(MergeMappingTuplesReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setCompressOutput(job, true);
		TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		job.setNumReduceTasks(numOfReducers);
		FileOutputFormat.setOutputPath(job, new Path(final_map_path));
		
		job.waitForCompletion(true);
		
	}
	
}
