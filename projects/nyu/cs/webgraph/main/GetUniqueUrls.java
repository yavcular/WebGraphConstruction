package nyu.cs.webgraph.main;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import nyu.cs.webgraph.MRhelpers.Commons;
import nyu.cs.webgraph.MRhelpers.SuperSampleIntervalSampler;
import nyu.cs.webgraph.MRhelpers.TabSeparatedLineRecordReader;
import nyu.cs.webgraph.MRhelpers.CustomFileInputFormats.LinkGraphInputFormat;
import nyu.cs.webgraph.MRhelpers.CustomFileInputFormats.TabSeperatedTextInputFormat;
import nyu.cs.webgraph.MRhelpers.CustomPartitioners.BaseDomainPartitioner;
import nyu.cs.webgraph.archiveUK.CreateLinkGraph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;

import com.hadoop.compression.lzo.LzoCodec;

public class GetUniqueUrls extends Configured implements Tool {
	
	
	public static final String NUM_OF_NOT_TO_REPLACE_ITEMS = "numOfSpaceSeperaredNoReplaceValues" ;
	public static int numOfReducers = 144;
	
	public static class GetUniqueUrlsMapper extends Mapper<Text, Text, Text, Text> {
		Text empty_txt = new Text(""); 
		Text url = new Text();
		String[] arr;
		int numNoReplaceValues; 
		
		public void setup(Context context){
			numNoReplaceValues = Integer.parseInt(context.getConfiguration().get(NUM_OF_NOT_TO_REPLACE_ITEMS));
		}
		
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			if (key.getLength() != 0)
				context.write(key, empty_txt);
			arr = value.toString().split(" ");
			for (int i = numNoReplaceValues; i < arr.length; i++) // loop doesn't start from 0 - so skip the no replace values
			{
				url.set(arr[i]);
				context.write(url, empty_txt);
			}
		}
	}
	
	public static class GetUniqueUrlsCombiner extends Reducer<Text, Text, Text, Text> {
		
		Text empty = new Text("");
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			context.write(key, empty);
		}
	}
	
	public static class GetUniqueUrlsReducer extends Reducer<Text, Text, Text, Text> {
		
		protected int counter; 
		protected boolean isFirst; 
		protected Text txt = new Text("");
		protected Text firtUrl = new Text("");
		protected void setup(Context context) throws IOException, InterruptedException {
			counter = 0;
			isFirst = true;
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			counter += 1;
			context.write(key, txt);
			if (isFirst)
			{
				firtUrl.set(key);
				isFirst = false;
			}
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(context.getConfiguration());
			Path pathForCounters = new Path(makeUUrlCountersPathName(FileOutputFormat.getOutputPath(context).toString())); 
			SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(pathForCounters, context.getTaskAttemptID().toString()), Text.class, Text.class);
			writer.append(new Text(firtUrl.toString()+"__"+ context.getTaskAttemptID().getTaskID().toString()), new Text(counter+""));
			writer.close(); 
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		System.out.println("args.length" + args.length);
		
		if (args.length != 5) {
			throw new IllegalArgumentException("Default file paths are in use\nUsage: <in_path> <out_path> <super_sample_path> <sample_for_partitioner_path> <numOfSpaceSeperaredNoReplaceValues>");
		}
		
		String path_slinkgraph = args[0], path_uurls = args[1], path_ssample = args[2], path_sampleforpartitioner = args[3], numOfNoReplaceValues = args[4]; 
		Path pathForCounters = new Path(makeUUrlCountersPathName(path_uurls));
		FileSystem fs = FileSystem.get(new Configuration());
		if (fs.exists(pathForCounters))
			fs.delete(pathForCounters, true);
		fs.mkdirs(pathForCounters);
		
		//first get the sample file for partitioner using supersample
		Sample.sampleForTotalOrderPatitioner(path_ssample, path_sampleforpartitioner, numOfReducers);
		System.out.println("Sample for partitioner is writen.");
		
		//run the job 
		Configuration conf = new Configuration();
		TotalOrderPartitioner.setPartitionFile(conf, new Path(path_sampleforpartitioner));
		conf.set(NUM_OF_NOT_TO_REPLACE_ITEMS, numOfNoReplaceValues );
		Job job = Job.getInstance(new Cluster(conf), conf);
		
		job.setJobName("get unique urls");
		job.setJarByClass(CreateLinkGraph.class);
		
		job.setMapperClass(GetUniqueUrlsMapper.class);
		job.setCombinerClass(GetUniqueUrlsCombiner.class);
		job.setReducerClass(GetUniqueUrlsReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(numOfReducers);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		job.setInputFormatClass(TabSeperatedTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(path_slinkgraph));
		FileOutputFormat.setOutputPath(job, new Path(path_uurls));
		
		TextOutputFormat.setCompressOutput(job, true);
		TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public static String makeUUrlCountersPathName(String uurlsPathName){
		return uurlsPathName + "-counters";
	}
}
