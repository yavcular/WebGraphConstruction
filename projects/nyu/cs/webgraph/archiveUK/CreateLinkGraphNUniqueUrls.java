package nyu.cs.webgraph.archiveUK;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import nyu.cs.webgraph.MRhelpers.Commons;
import nyu.cs.webgraph.MRhelpers.CustomFileInputFormats.RawToLinkInputFormat;
import nyu.cs.webgraph.MRhelpers.CustomPartitioners.BaseDomainPartitioner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**
 * This MR job converts raw web graph to following format
 * "sourceURL destURL destURL ..."
 */
public class CreateLinkGraphNUniqueUrls extends Configured implements Tool {
	
	public static String LinkgraphPath;
	
	private static final Log LOG = LogFactory.getLog(CreateLinkGraphNUniqueUrls.class);
	
	
	
	public static class GMapper extends Mapper<Text, Text, Text, NullWritable> {
		
		Path path;
		//SequenceFile.Writer writer;
		BufferedWriter writer;
		
		FileSystem fs;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			path = new Path("/user/yasemin/webgraph/output/ie-historical/linkgraph/" + context.getTaskAttemptID());
			fs = FileSystem.get(context.getConfiguration());
			CompressionCodec codec = new GzipCodec();
			//writer = SequenceFile.createWriter(fs, context.getConfiguration(), path, Text.class, Text.class, CompressionType.RECORD, codec);
			//writer = SequenceFile.createWriter(fs, context.getConfiguration(), path, Text.class, Text.class);
			writer = new BufferedWriter(new OutputStreamWriter(fs.create(path)));
		}
		
		NullWritable nw = NullWritable.get();
		String[] arr;
		Text destUrl = new Text();
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			if (key.find("filedesc") != 0) // if !key.startswith(filedesc)
			{
				/*linkgraph*/
				if (key != null && key.getLength() != 0)
					//writer.append(key, value);
					writer.append(key.toString() + "\t" + value.toString()+"\n");
					
				/*unique urls*/
				if (key.getLength() != 0)
					context.write(key, nw);
				arr = value.toString().split(" ");
				for (int i = 1; i < arr.length; i++) // loop starts from 1 - so skip the time 
				{
					destUrl.set(arr[i]);
					context.write(destUrl, nw);
				}
			}
		}
		protected void cleanup(Context context) throws IOException, InterruptedException {
			writer.close();
		}
	}
	
	public static class GReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, values.iterator().next());
		}
	}
	
	public static void main(String[] args) throws Exception {
		System.err.println("create linkgraph & unique urls YAYY!! \n");
		
		boolean isDefaultFilePath = false;
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Default file paths are in use\nUsage: RawGraphToLinkGraph <in> <linkgraph_dir> <uniqueurls_dir>");
			isDefaultFilePath = true;
		}
		Job job =  new Job(conf, "create linkgraph & unique urls");
		job.setJarByClass(CreateLinkGraphNUniqueUrls.class);
		
		job.setInputFormatClass(RawToLinkInputFormat.class);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		//FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		//FileOutputFormat.setCompressOutput(job, true);
		
		job.setMapperClass(GMapper.class);
		job.setReducerClass(GReducer.class);
		job.setCombinerClass(GReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(40); //0.95 or 1.75 * (nodes (11 beaker nodes) * mapred.tasktracker.tasks.maximum (default 2) )
		job.setPartitionerClass(BaseDomainPartitioner.class);
		
		if (isDefaultFilePath){
			FileInputFormat.addInputPath(job, new Path(Commons.METADATA_PATH));
			LinkgraphPath = Commons.LINKGRAPH_PATH;
			FileOutputFormat.setOutputPath(job, new Path(Commons.UNIQUEURLS_PATH));
		}
		else
		{
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			LinkgraphPath = otherArgs[1];
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		}
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
}
