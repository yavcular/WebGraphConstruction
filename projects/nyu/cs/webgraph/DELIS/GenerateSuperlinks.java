package nyu.cs.webgraph.DELIS;

import java.io.IOException;
import java.util.Random;

import nyu.cs.webgraph.MRhelpers.BufferedTextOutputFormat;
import nyu.cs.webgraph.MRhelpers.CustomFileInputFormats.LinkGraphInputFormat;
import nyu.cs.webgraph.structures.BitVectorArray;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.hadoop.compression.lzo.LzoCodec;

public class GenerateSuperlinks {
	public static String OLidentifier = "ol-";
	public static String BVidentifier = "bv-";
	public static String[] timeIdentifier = {"200606", "200607", "200608", "200609", "200610", "200611", "200612", "200701", "200702", "200703", "200704", "200705"};
	
	public static String appendIdentifier(String actualVal, String identifier){
		return new StringBuffer(identifier).append(actualVal).toString();
	}
	public static String removeIdentifier(String str, int identifierLength){
		return str.substring(identifierLength);
	}
	
	public static class GenerateOLs extends Mapper<Text, Text, Text, Text> {

		Text txt = new Text();

		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			txt.set(appendIdentifier(value.toString(), OLidentifier));
			context.write(key, txt);
		}
	}
	
	public static class GenerateBVs extends Mapper<Text, Text, Text, Text> {
		
		Text txt = new Text();

		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			txt.set(appendIdentifier(value.toString(), BVidentifier));
			context.write(key, txt);

		}
	}
	
	public static class SuperLinkReducer extends Reducer<Text, Text, Text, Text> {
		
		private MultipleOutputs mos;
		
		String bvStr, olStr;
		String[] bvArr, olArr;
		StringBuffer[] OLsPerMonth;
		
		Text txt = new Text();
		Random rndm = new Random();
		public void setup(Context context ){
			mos = new MultipleOutputs(context);
		}
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			String curr;
			for (Text val : values) {
				curr = val.toString();
				if (curr.startsWith(BVidentifier))
					bvStr = curr;
				else if (curr.startsWith(OLidentifier))
					olStr = curr;
			}
			
			bvArr = removeIdentifier(bvStr, BVidentifier.length()).split(" ");
			olArr = removeIdentifier(olStr, OLidentifier.length()).split(" ");
			char[] currBV;
			OLsPerMonth = new StringBuffer[12];
			for (int i = 0; i < OLsPerMonth.length; i++) {
				OLsPerMonth[i] = new StringBuffer();
			}
			
			if (olArr.length != bvArr.length)
				throw new IllegalArgumentException("bv and ol values do not match for document" + key.toString() + " bv[].length:" +bvArr.length + "ol[].length" + olArr.length);
			
			for (int i = 0; i < olArr.length; i++) {
				
				currBV = bvArr[i].toCharArray();
				for (int j = 0; j < currBV.length; j++) {
					if (currBV[j] == '1')
					{
						OLsPerMonth[j].append(olArr[i] + " ");
					}
				}
			}
				
			long seed = rndm.nextLong();
			rndm.setSeed(seed);
			double freq = (double) ((double) 10 / (double)133);
			// sample source only
			if (rndm.nextDouble() <= freq) {
				for (int i = 0; i < OLsPerMonth.length; i++) {
					txt.set(timeIdentifier[i] + " " + OLsPerMonth[i].toString());
					mos.write(timeIdentifier[i], key, txt);
				}
			}
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		String olPath = otherArgs[0];
		String bvPath = otherArgs[1];
		String outputPath = otherArgs[2];
		
		Job job = Job.getInstance(new Cluster(conf), conf);
		job.setJobName("generate superlinks from bv and ol layers");
		job.setJarByClass(GenerateSuperlinks.class);
		
		MultipleInputs.addInputPath(job, new Path(olPath), LinkGraphInputFormat.class, GenerateOLs.class);
		MultipleInputs.addInputPath(job, new Path(bvPath), LinkGraphInputFormat.class, GenerateBVs.class);
		job.setReducerClass(SuperLinkReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		TextOutputFormat.setCompressOutput(job, true);
		TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		for (int i = 0; i < timeIdentifier.length; i++) {
			MultipleOutputs.addNamedOutput(job, timeIdentifier[i], TextOutputFormat.class, Text.class, Text.class);
		}
		
		job.setNumReduceTasks(48);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}
}