package nyu.cs.webgraph.archiveUK;

import java.io.IOException;

import nyu.cs.webgraph.MRhelpers.CustomFileInputFormats.TabSeperatedTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RepartitionLayers {


	public static class IdentitiyMapper extends Mapper<Text, Text, LongWritable, Text> {
		
		LongWritable lw = new LongWritable();
		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			lw.set(Long.parseLong(key.toString()));
			context.write(lw, value);
		}
	}
	
	public static class IdentityReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			for (Text text : values) {
				context.write(key, text);
			}
			
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		if (args.length != 3)
			throw new IllegalArgumentException(args.length + " usage: inpath, outpath, partitionerpath ");
		String inPath = args[0], outPath = args[1], samplePath = args[2];
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path( samplePath))) {
			fs.delete(new Path(samplePath), false);
		}
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(samplePath), LongWritable.class, NullWritable.class);
		LongWritable lw = new LongWritable();
		int total = 928000000, numofMappers = 143;
		int increaseAmount = total/numofMappers;
		int counter =increaseAmount;
		while(counter < total){
			lw.set(counter);
			writer.append(lw, NullWritable.get());
			counter+=increaseAmount;
		}
		writer.close();
		
		conf = new Configuration();
		TotalOrderPartitioner.setPartitionFile(conf, new Path(samplePath));
		
		Job job = Job.getInstance(new Cluster(conf), conf);
		job.setJobName("repartition layers");
		job.setJarByClass(RepartitionLayers.class);

		job.setMapperClass(IdentitiyMapper.class);
		job.setReducerClass(IdentityReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TabSeperatedTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		FileInputFormat.setInputPaths(job, new Path(inPath));
		
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		job.setNumReduceTasks(144);
		job.waitForCompletion(true);
		
	}
	
	
}
