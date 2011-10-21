package nyu.cs.webgraph.updates;

import java.io.IOException;

import nyu.cs.webgraph.MRhelpers.CustomFileInputFormats.TabSeperatedTextInputFormat;
import nyu.cs.webgraph.main.Sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class SourceOnlyReplace {
	
	public static final String NOREP_IDENTIFIER = "n_", MAPPING_IDENTIFIER = "m_";
	public static final String IS_KEY_FIRST_IN_MAP_FILE = "isKeyFirst";
	
	public static String appendMappingIdentifier(String id){
		return MAPPING_IDENTIFIER + id;
	}
	public static String appendNoREpIdentifier(String id){
		return NOREP_IDENTIFIER + id;
	}
	public static String removeMappingIdentifier(String str){
		return str.substring(MAPPING_IDENTIFIER.length());
	}
	public static String removeNoRepIdentifier(String str){
		return str.substring(NOREP_IDENTIFIER.length());
	}
	public static boolean isMapping(String str){
		return str.startsWith(MAPPING_IDENTIFIER);
	}
	public static boolean isNoRep(String str){
		return str.startsWith(NOREP_IDENTIFIER);
	}
	
	public static class NoRepMapper extends Mapper<Text, Text, LongWritable, Text> {
		
		Text val = new Text();
		LongWritable lw = new LongWritable();
		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			val.set(appendNoREpIdentifier(value.toString()));
			lw.set(Long.parseLong(key.toString()));
			context.write(lw, val);
		}
	}
	public static class MappingMapper extends Mapper<Text, Text, LongWritable, Text> {
		Text val = new Text();
		LongWritable lw = new LongWritable();
		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			
			if (Boolean.parseBoolean(context.getConfiguration().get(IS_KEY_FIRST_IN_MAP_FILE))){
				lw.set(Long.parseLong(key.toString()));
				val.set(appendMappingIdentifier(value.toString()));
				context.write(lw, val);
			}
			else{
				lw.set(Long.parseLong(value.toString()));
				val.set(appendMappingIdentifier(key.toString()));
				context.write(lw, val);
			}
				
		}
	}
	
	public static class ReplaceReducer extends Reducer<LongWritable, Text, LongWritable, Text>{
		Text k = new Text();
		Text v = new Text();
		boolean hasMapping, hasNoRep; 
		LongWritable lw = new LongWritable();
		String str;
		StringBuffer sb = new StringBuffer();
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			hasMapping = false; hasNoRep = false;
			int count = 0;
			for (Text val : values) {
				count++;
				str = val.toString();
				  if (isMapping(str)){
					hasMapping = true;
					k.set(removeMappingIdentifier(str));
				}
				else if (isNoRep(str)){
					hasNoRep = true;
					v.set(removeNoRepIdentifier(str));
				}
				else
					throw new IllegalArgumentException("unknown value: " + str);
				
				  if (count == 3)
					  throw new IllegalArgumentException("too many values: " + str + " key:" +k.toString() + " v:" + v.toString());
			}
			if (hasMapping && hasNoRep){
				lw.set(Long.parseLong(k.toString()));				
				context.write(lw, v);
			}
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		if (args.length != 5)
			throw new IllegalArgumentException("Incorrect number of parameters(" + args.length+ "): <source_path>, <mapping_path>, <isKeyFirst> <output_path> <numericsample_path> ");
		
		String source_path  = args[0], mapping_path = args[1], isKeyFirst = args[2], output_path = args[3];
		String numeric_sample_path = args[4];
		
		int numberOfReducers = MergeUrlIdMaps.numOfReducers;
		
		Configuration conf = new Configuration();
		TotalOrderPartitioner.setPartitionFile(conf, new Path(numeric_sample_path));
		conf.set(IS_KEY_FIRST_IN_MAP_FILE, isKeyFirst);
		
		Job job = Job.getInstance(new Cluster(conf), conf);
		job.setJobName("source only replace");
		job.setJarByClass(SourceOnlyReplace.class);
		
		MultipleInputs.addInputPath(job, new Path(source_path), TabSeperatedTextInputFormat.class, NoRepMapper.class);
		MultipleInputs.addInputPath(job, new Path(mapping_path), TabSeperatedTextInputFormat.class, MappingMapper.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		job.setReducerClass(ReplaceReducer.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		TextOutputFormat.setCompressOutput(job, true);
		TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		job.setNumReduceTasks(numberOfReducers);
		FileOutputFormat.setOutputPath(job, new Path(output_path));
		
		job.waitForCompletion(true);
		
	}
	
}