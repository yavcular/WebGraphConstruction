package nyu.cs.webgraph.updates;

import java.io.IOException;

import nyu.cs.webgraph.MRhelpers.CustomFileInputFormats.TabSeperatedTextInputFormat;
import nyu.cs.webgraph.main.PrepareFinalLayerFiles;
import nyu.cs.webgraph.main.Sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;


//given merged mapping, generates two translation tables
public class TTables {
	
	// input is link-graph
	public static class IdentityMapper extends Mapper<Text, Text, LongWritable, Text> {
		
		LongWritable lw = new LongWritable();
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			lw.set(Long.parseLong(key.toString()));
			context.write(lw, value);
		}
	}
	
	public static class TTableReducer extends Reducer<LongWritable, Text, Text, Text> {
		
		
		private MultipleOutputs mos;
		Text tmpText = new Text();
		Text keytxt = new Text();
		
		public void setup(Context context) {
			mos = new MultipleOutputs(context);
		}
		
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String[] value = values.iterator().next().toString().split(" ");
			String current_id = null, new_id = null, keyUrl = ""; 
			keytxt.set(key.get()+"");
			
			for (int i = value.length-1; i >= 0; i--) {
				if(value[i].startsWith(MergeUrlIdMaps.CURRENT_MAP_IDENTIFIER))
					current_id = value[i].substring(MergeUrlIdMaps.CURRENT_MAP_IDENTIFIER.length()); 
				else if (value[i].startsWith(MergeUrlIdMaps.NEW_MAP_IDENTIFIER))
					new_id = value[i].substring(MergeUrlIdMaps.NEW_MAP_IDENTIFIER.length());
				else 
					keyUrl += value[i];
			}
			
			if (current_id != null){
				tmpText.set(current_id);
				mos.write("ttcurrent", key, tmpText);
			}
			if (new_id != null){
				tmpText.set(new_id);
				mos.write("ttnew", key, tmpText);
			}
			
			tmpText.set(keyUrl);
			mos.write("newmapping", key, tmpText);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		String inpath, outpath, numericsamplepath, offsets_path; 
		if (args.length != 4) {
			throw new IllegalArgumentException("arguments: inpath, outpath, offsetspath " + args.length);
		}
		inpath = args[0];
		outpath = args[1]; 
		numericsamplepath = args[2];
		offsets_path = args[3];
		
		Configuration conf = new Configuration();
		TotalOrderPartitioner.setPartitionFile(conf, new Path( numericsamplepath));
		
		Job job = new Job(conf, "get translation tables");
		job.setJarByClass(PrepareFinalLayerFiles.class);
		
		job.setMapperClass(IdentityMapper.class);
		job.setReducerClass(TTableReducer.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setPartitionerClass(TotalOrderPartitioner.class);
		job.setInputFormatClass(TabSeperatedTextInputFormat.class);
		
		MultipleOutputs.addNamedOutput(job, "ttcurrent", TextOutputFormat.class , Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ttnew", TextOutputFormat.class , Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "newmapping", TextOutputFormat.class , Text.class, Text.class);
		
		TextOutputFormat.setCompressOutput(job, true);
		TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		FileInputFormat.addInputPath(job, new Path(inpath));
		FileOutputFormat.setOutputPath(job, new Path(outpath));
		
		//generate a numeric sampling using offsets for partitioning
		int numOfReducers = Sample.writeNumericSampleForTOP(offsets_path, numericsamplepath, conf, job);
		job.setNumReduceTasks(numOfReducers);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
}
