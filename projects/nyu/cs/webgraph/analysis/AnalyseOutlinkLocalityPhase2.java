package nyu.cs.webgraph.analysis;

import java.io.IOException;
import java.util.HashSet;

import nyu.cs.webgraph.MRhelpers.UrlManipulation;
import nyu.cs.webgraph.MRhelpers.CustomFileInputFormats.TabSeperatedTextInputFormat;
import nyu.cs.webgraph.analysis.OutlinkGrowthAnalysis.AnaylseOLGrowthReducer;
import nyu.cs.webgraph.analysis.OutlinkGrowthAnalysis.BVIdentitiyMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AnalyseOutlinkLocalityPhase2 {

	public static class RemoteSiteCountMapper extends Mapper<Text, Text, Text, Text> {

		String[] curr_stats;
		Text k_uniqueREmoteSiteCount = new Text();
		Text v_localAndRemoteCount = new Text();
		String curr_key;
		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			
			curr_key = key.toString();
			curr_stats = curr_key.split(" ");
			if (curr_stats.length == 3){
				k_uniqueREmoteSiteCount.set(curr_stats[2]);
				v_localAndRemoteCount.set(curr_stats[0] + " " + curr_stats[1]);
			}
			else
			{
				k_uniqueREmoteSiteCount.set("salak");
				v_localAndRemoteCount.set(curr_key + " length" + curr_stats.length);
			}
			context.write(k_uniqueREmoteSiteCount, v_localAndRemoteCount);
		}
	}
	
	public static class SumupReducer extends Reducer<Text, Text, Text, Text> {

/*		private MultipleOutputs mos;

		public void setup(Context context) {
			mos = new MultipleOutputs(context);
		}
*/		
		Text v_localAndRemoteCount = new Text();
		int uniqueSiteCounter=0, glocalLocalLinkCounter=0, globalReoteLinkCounter=0;
		String[] localAndRemoteCounts;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (key.toString().equals("salak")) {
				for (Text text : values) {
					context.write(key, text);
				}
			} else {
				uniqueSiteCounter = 0;
				globalReoteLinkCounter = 0;
				glocalLocalLinkCounter = 0;
				for (Text text : values) {
					uniqueSiteCounter++;
					localAndRemoteCounts = text.toString().split(" ");
					if (localAndRemoteCounts.length == 2) {
						glocalLocalLinkCounter += Integer.parseInt(localAndRemoteCounts[0]);
						globalReoteLinkCounter += Integer.parseInt(localAndRemoteCounts[1]);
					}
				}
				v_localAndRemoteCount.set(uniqueSiteCounter + " " + glocalLocalLinkCounter + " " + globalReoteLinkCounter);
				context.write(key, v_localAndRemoteCount);
			}
		}
		
	/*	protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.write("removed", key, new Text(removedLinksList.toString()));
			mos.close();
		}*/
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		if (args.length != 2)
			throw new IllegalArgumentException(args.length + " usage: ... ");
		
		String inPath = args[0], outputPath = args[1];
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(new Cluster(conf), conf);
		job.setJobName("locality analyses - phase2");
		job.setJarByClass(AnalyseOutlinkLocalityPhase2.class);

		job.setMapperClass(RemoteSiteCountMapper.class);
		job.setReducerClass(SumupReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
		
	}
	
}
