package nyu.cs.webgraph.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.TreeSet;

import nyu.cs.webgraph.MRhelpers.BufferedTextOutputFormat;
import nyu.cs.webgraph.MRhelpers.Commons;
import nyu.cs.webgraph.MRhelpers.CustomFileInputFormats.TabSeperatedTextInputFormat;
import nyu.cs.webgraph.MRhelpers.CustomPartitioners.NumericBasePartitioner;
import nyu.cs.webgraph.structures.BitVectorArray;
import nyu.cs.webgraph.structures.SingleCrawl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

public class PrepareFinalLayerFiles  extends Configured implements Tool {

	// input is link-graph
	public static class FinalLayersMapper extends Mapper<Text, Text, LongWritable, Text> {
		
		Text pageCrawled = new Text();
		Text outlinks = new Text();
		LongWritable pageid = new LongWritable();
		
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			if (key.getLength() > 0)
			{
				pageid.set(Long.parseLong(key.toString()));
				context.write(pageid, value);
			}
		}
	}
	
	public static class FinalLayersReducer extends Reducer<LongWritable, Text, WritableComparable, Writable> {
		
		private MultipleOutputs mos;
		
		LongWritable lw = new LongWritable();
		IntWritable iw = new IntWritable();
		String[] curr_links;
		ArrayList<SingleCrawl> crawlInfo = new ArrayList<SingleCrawl>();
		TreeSet<String> outlinks_total = new TreeSet<String>();
		//BitVector[] bitVectorArr;
		//BitVector bitVector; 
		BitVectorArray bitVectorArr;
		public void setup(Context context) {
			mos = new MultipleOutputs(context);
		}
		
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			outlinks_total.clear();
			crawlInfo.clear();
			StringBuffer strbuff;
			
			iw.set((int)key.get());
			
			for (Text val : values) {
				curr_links = val.toString().split(" ");
				SingleCrawl crawl = new SingleCrawl();
				crawl.setTimeStamp(Long.parseLong(curr_links[0]));
				for (int i = 1; i < curr_links.length; i++) {
					crawl.addOutlink(curr_links[i]);
					outlinks_total.add(curr_links[i]);
				}
				crawlInfo.add(crawl);
			}
			
			Collections.sort(crawlInfo);
			strbuff = new StringBuffer("");
			for (int i = 0; i < crawlInfo.size(); i++) {
				strbuff.append(crawlInfo.get(i).getTimestamp() + " ");
			}
			mos.write("timestamp", iw, new Text(strbuff.toString()));
			
			strbuff = new StringBuffer("");
			Iterator<String> iter = outlinks_total.iterator();
			while (iter.hasNext()) {
				strbuff.append(iter.next() + " ");
			}
			mos.write("outlink", iw, new Text(strbuff.toString()));
			
			bitVectorArr = new BitVectorArray(crawlInfo.size(), outlinks_total.size());
			int currentbit;
			for (int i = 0; i < crawlInfo.size(); i++) {
				//bitVectorArr[i] = new BitVector(outlinks_total.size());
				currentbit =0;
				iter = outlinks_total.iterator();
				while(iter.hasNext()){
					if (crawlInfo.get(i).hasLink(iter.next()))
					{
						//bitVectorArr[i].set(currentbit);
						bitVectorArr.set(i, currentbit);
					}
					currentbit++;
				}
			}
			mos.write("bitvector", iw, bitVectorArr);
			//mos.write("bitvector", iw, new Text("bitvector:" + bitVector.size() + " numOfCrawls" + crawlInfo.size()));
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		String inpath, outpath, offsetspath; 
		if (args.length != 3) {
			throw new IllegalArgumentException("arguments: inpath, outpath, offsetspath");
		}
		inpath = args[0];
		outpath = args[1]; 
		offsetspath = args[2];
		
		Configuration conf = new Configuration();
		TotalOrderPartitioner.setPartitionFile(conf, new Path( new Path(inpath).getParent(), "numericsample" ));
		
		Job job = new Job(conf, "prepare final layer files");
		job.setJarByClass(PrepareFinalLayerFiles.class);
		System.out.println(new Path( new Path(inpath).getParent(), "numericsample" ).toString());
		
		job.setMapperClass(FinalLayersMapper.class);
		job.setReducerClass(FinalLayersReducer.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		//job.setPartitionerClass(NumericBasePartitioner.class);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		job.setInputFormatClass(TabSeperatedTextInputFormat.class);
		
		MultipleOutputs.addNamedOutput(job, "outlink", TextOutputFormat.class , IntWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "timestamp", TextOutputFormat.class , IntWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "bitvector", BufferedTextOutputFormat.class , IntWritable.class, BitVectorArray.class);
		
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		
		FileInputFormat.addInputPath(job, new Path(inpath));
		FileOutputFormat.setOutputPath(job, new Path(outpath));
		

		//generate a numeric sampling using offsets for partitioning
		int count_offsets = Sample.writeNumericSampleForTOP(offsetspath, TotalOrderPartitioner.getPartitionFile(job.getConfiguration()), conf, job); 
		
		job.setNumReduceTasks(count_offsets);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
