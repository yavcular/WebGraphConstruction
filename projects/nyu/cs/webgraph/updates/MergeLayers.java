package nyu.cs.webgraph.updates;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;

import nyu.cs.webgraph.MRhelpers.BufferedTextOutputFormat;
import nyu.cs.webgraph.structures.BitVectorArray;
import nyu.cs.webgraph.structures.SingleCrawlOutlinkList;
import nyu.cs.webgraph.structures.VersionedCrawls;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class MergeLayers {

	public static String TS1_INDENTIFIER = "ts1_";
	public static String TS2_INDENTIFIER = "ts2_";
	public static String OL1_INDENTIFIER = "ol1_";
	public static String OL2_INDENTIFIER = "ol2_";
	public static String BV1_INDENTIFIER = "bv1_";
	public static String BV2_INDENTIFIER = "bv2_";

	public static String appendTS1Identifier(String str){
		return TS1_INDENTIFIER+ str;
	}
	public static String appendTS2Identifier(String str){
		return TS2_INDENTIFIER+ str;
	}
	public static String appendOL1Identifier(String str){
		return OL1_INDENTIFIER+ str;
	}
	public static String appendOL2Identifier(String str){
		return OL2_INDENTIFIER+ str;
	}
	public static String appendBV1Identifier(String str){
		return BV1_INDENTIFIER+ str;
	}
	public static String appendBV2Identifier(String str){
		return BV2_INDENTIFIER+ str;
	}
	
	public static boolean isTS(String str) {
		return str.startsWith(TS1_INDENTIFIER) || str.startsWith(TS2_INDENTIFIER);
	}
	public static boolean isTS1(String str) {
		return str.startsWith(TS1_INDENTIFIER);
	}
	public static boolean isOL(String str) {
		return str.startsWith(OL1_INDENTIFIER) || str.startsWith(OL2_INDENTIFIER);
	}
	public static boolean isOL1(String str) {
		return str.startsWith(OL1_INDENTIFIER);
	}
	public static boolean isBV(String str) {
		return str.startsWith(BV1_INDENTIFIER) || str.startsWith(BV2_INDENTIFIER);
	}
	public static boolean isBV1(String str) {
		return str.startsWith(BV1_INDENTIFIER);
	}

	public static String removeIdentifier(String str, String identifier) {
		return str.substring(identifier.length());
	}
	public static ArrayList<String> toArrayList(String[] arr){
		ArrayList<String> al = new ArrayList<String>();
		for (int i = 0; i < arr.length; i++) {
			al.add(arr[i]);
		}
		return al;
	}
	
	public static class OL1Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		Text val = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			val.set(appendOL1Identifier(value.toString()));
			context.write(key, val);
		}
	}
	
	public static class OL2Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		Text val = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			val.set(appendOL2Identifier(value.toString()));
			context.write(key, val);
		}
	}

	public static class BV1Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		Text val = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			val.set(appendBV1Identifier(value.toString()));
			context.write(key, val);
		}
	}

	public static class BV2Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		Text val = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			val.set(appendBV2Identifier(value.toString()));
			context.write(key, val);
		}
	}

	public static class TS1Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		Text val = new Text();
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			val.set(appendTS1Identifier(value.toString()));
			context.write(key, val);
		}
	}
	
	public static class TS2Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		Text val = new Text();
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			val.set(appendTS2Identifier(value.toString()));
			context.write(key, val);
		}
	}
	
	public static class FinalLayersReducer extends Reducer<LongWritable, Text, Text, Text> {

		private MultipleOutputs mos;
		StringBuffer outbuff = new StringBuffer();
		Text k = new Text();
		
		TreeSet<String> olsUnion = new TreeSet<String>();
		TreeSet<String> tssUnion = new TreeSet<String>();
		BitVectorArray bitvectors;
		String curr_val = "";
		
		VersionedCrawls currentLayers = new VersionedCrawls();
		VersionedCrawls newLayers = new VersionedCrawls();
		
		public void setup(Context context) {
			mos = new MultipleOutputs(context);
		}
		
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			k.set(key.get() + "");
			tssUnion.clear();
			olsUnion.clear();
			currentLayers.clear();
			newLayers.clear();
			int valcount=0;
			
			for (Text val : values) {
				valcount++;
				curr_val = val.toString();
				if (isTS(curr_val)) {
					tssUnion.addAll(toArrayList(removeIdentifier(curr_val, TS1_INDENTIFIER).split(" ")));
					if (isTS1(curr_val))
						currentLayers.addTimestamps(removeIdentifier(curr_val, TS1_INDENTIFIER).split(" "));
					else
						newLayers.addTimestamps(removeIdentifier(curr_val, TS2_INDENTIFIER).split(" "));
				} else if (isOL(curr_val)) {
					olsUnion.addAll(toArrayList(removeIdentifier(curr_val, OL1_INDENTIFIER).split(" ")));
					if (isOL1(curr_val))
						currentLayers.addOutlink(removeIdentifier(curr_val, OL1_INDENTIFIER).split(" "));
					else
						newLayers.addOutlink(removeIdentifier(curr_val, OL2_INDENTIFIER).split(" "));
				} else if (isBV(curr_val)) {
					if (isBV1(curr_val))
						currentLayers.addBitvectors(removeIdentifier(curr_val, BV1_INDENTIFIER).split(" "));
					else
						newLayers.addBitvectors(removeIdentifier(curr_val, BV2_INDENTIFIER).split(" "));
				}
			}
			
			
			//write out timestamp layer
			outbuff = new StringBuffer();
			Iterator<String> itr = tssUnion.iterator();
			while (itr.hasNext()) {
				outbuff.append(itr.next()).append(" ");
			}
			mos.write("timestamp", k, new Text(outbuff.toString()));
			
			
			//write out outlink layer
			outbuff = new StringBuffer();
			itr = olsUnion.iterator();
			while (itr.hasNext()) {
				outbuff.append(itr.next()).append(" ");
			}
			mos.write("outlink", k, new Text(outbuff.toString()));

			
			currentLayers.construct();
			newLayers.construct();
			Iterator<String> tsUnionItr = tssUnion.iterator();
			Iterator<String> olUnionItr = olsUnion.iterator();
			bitvectors = new BitVectorArray(tssUnion.size(), olsUnion.size());
			String curr_ts, curr_ol; 
			int ts_pos=0, ol_pos=0;
			
			while (tsUnionItr.hasNext()) {
				curr_ts = tsUnionItr.next();
				olUnionItr = olsUnion.iterator();
				ol_pos =0;
				while (olUnionItr.hasNext()) {
					curr_ol = olUnionItr.next();
					if (currentLayers.hasLink(curr_ts, curr_ol) || newLayers.hasLink(curr_ts, curr_ol)) {
						bitvectors.set(ts_pos, ol_pos);
					}
					ol_pos++;
				}
				ts_pos++;
			}
			mos.write("bitvector", k, bitvectors);
			
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}
	
	public static void main(String[] args) throws Exception {

		if (args.length != 8) {
			throw new IllegalArgumentException("arguments: inpath, outpath, offsetspath");
		}
		String curr_ol_inpath = args[0];
		String curr_ts_inpath = args[1];
		String curr_bv_inpath = args[2];
		
		String new_ol_inpath = args[3];
		String new_ts_inpath = args[4];
		String new_bv_inpath = args[5];
		
		String out_path = args[6];
		String numeric_sample_path = args[7];
		
		Configuration conf = new Configuration();
		TotalOrderPartitioner.setPartitionFile(conf, new Path(numeric_sample_path));
		
		Job job = new Job(conf, "merge updated layers");
		job.setJarByClass(MergeLayers.class);

		job.setReducerClass(FinalLayersReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		MultipleInputs.addInputPath(job, new Path(curr_ol_inpath), SequenceFileInputFormat.class, OL1Mapper.class);
		MultipleInputs.addInputPath(job, new Path(curr_ts_inpath), SequenceFileInputFormat.class, TS1Mapper.class);
		MultipleInputs.addInputPath(job, new Path(curr_bv_inpath), SequenceFileInputFormat.class, BV1Mapper.class);
		MultipleInputs.addInputPath(job, new Path(new_ol_inpath), SequenceFileInputFormat.class, OL2Mapper.class);
		MultipleInputs.addInputPath(job, new Path(new_ts_inpath), SequenceFileInputFormat.class, TS2Mapper.class);
		MultipleInputs.addInputPath(job, new Path(new_bv_inpath), SequenceFileInputFormat.class, BV2Mapper.class);
		
		MultipleOutputs.addNamedOutput(job, "outlink", TextOutputFormat.class, IntWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "timestamp", TextOutputFormat.class, IntWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "bitvector", BufferedTextOutputFormat.class, IntWritable.class, BitVectorArray.class);

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

		FileOutputFormat.setOutputPath(job, new Path(out_path));

		job.setNumReduceTasks(MergeUrlIdMaps.numOfReducers);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
