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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class AnalyseOutlinkLocality {

	
	
	
	public static class OLIdentitiyMapper extends Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	
	public static class LocalityReducer extends Reducer<Text, Text, Text, Text> {

/*		private MultipleOutputs mos;

		public void setup(Context context) {
			mos = new MultipleOutputs(context);
		}
*/		
		Text finalStats = new Text("");
		Text empty = new Text("");
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			String[] olArr = null;
			HashSet< String> outlinks = new HashSet<String>();
			
			for (Text text : values) {
				olArr = text.toString().split(" ");
				if (olArr.length < 1)
					continue;
				for (int i = 1; i < olArr.length; i++) {
					if (olArr[i].length() !=0)
						outlinks.add(olArr[i]);
				}
			}
			
			
			if (outlinks.size() > 0){
				
				String sourceBase = UrlManipulation.getBaseDomain(key.toString());
				int localLinks=0, remoteLinks=0; 
				HashSet< String> remoteDomains = new HashSet<String>();
				String curr_domain;
				
				for (String url : outlinks) {
					curr_domain = UrlManipulation.getBaseDomain(url);
					if (curr_domain.length() != 0){
						if ( curr_domain.equalsIgnoreCase(sourceBase)){
							localLinks++;
						}
						else{
							remoteLinks++;
							remoteDomains.add(curr_domain);
						}
					}
				}
				
				/*StringBuffer strBuff = new StringBuffer("source_"+sourceBase + " dests_");
				for (String string : remoteDomains) {
					strBuff.append(string + " ");
				}
				empty.set(strBuff.toString());*/
				finalStats.set(localLinks + " " + remoteLinks + " " + remoteDomains.size());
				context.write(finalStats, empty);
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
		
		String strOutlinkPath = args[0], outputPath = args[1];
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(new Cluster(conf), conf);
		job.setJobName("locality analyses");
		job.setJarByClass(AnalyseOutlinkLocality.class);

		job.setMapperClass(OLIdentitiyMapper.class);
		job.setReducerClass(LocalityReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TabSeperatedTextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileInputFormat.setInputPaths(job, new Path(strOutlinkPath));
		
		job.setNumReduceTasks(48);
		job.waitForCompletion(true);
		
	}
	
}
