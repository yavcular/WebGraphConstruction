package nyu.cs.webgraph.analysis;

import java.io.IOException;
import java.util.ArrayList;

import nyu.cs.webgraph.MRhelpers.CustomFileInputFormats.TabSeperatedTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class OutlinkGrowthAnalysis {

	public static class BVIdentitiyMapper extends Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class AnaylseOLGrowthReducer extends Reducer<Text, Text, Text, Text> {

		int thereIsAChange, onlyAdded, onlyRemoved, bothAddedAndRemoved, totalIncreased, totalDecresed, nochange;

		public void setup(Context context) {
			thereIsAChange = 0;
			onlyAdded = 0;
			onlyRemoved = 0;
			bothAddedAndRemoved = 0;
			totalIncreased = 0;
			totalDecresed = 0;
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String[] bvArr;
			String currBV, nextBV;

			boolean added = false;
			boolean removed = false;
			int totalChange = 0;

			for (Text text : values) {
				bvArr = text.toString().split(" ");
				if (bvArr.length < 2) {
					nochange++;
					continue;
				}

				// see what happened
				added = false;
				removed = false;
				totalChange = 0;

				for (int i = 0; i < bvArr.length - 1; i++) {
					currBV = bvArr[i];
					nextBV = bvArr[i + 1];

					for (int j = 0; j < currBV.length(); j++) {
						if (currBV.charAt(j) != nextBV.charAt(j)) {
							if (currBV.charAt(j) == '1') {
								removed = true;
								totalChange--;
							} else if (currBV.charAt(j) == '0') {
								added = true;
								totalChange++;
							}
						}
					}
				}

				// update global variable according to what happened
				if (!added && !removed) {
					nochange++;
				} else {
					thereIsAChange++;
					if (added && !removed)
						onlyAdded++;
					else if (!added && removed)
						onlyRemoved++;
					else
						bothAddedAndRemoved++;
					if (totalChange > 0)
						totalIncreased++;
					else if (totalChange < 0)
						totalDecresed++;
				}

			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {

			String[] keys = { "thereIsAChange", "onlyAdded", "onlyRemoved", "bothAddedAndRemoved", "totalIncreased", "totalDecresed", "nochange" };
			int[] values = { thereIsAChange, onlyAdded, onlyRemoved, bothAddedAndRemoved, totalIncreased, totalDecresed, nochange };
			Text key = new Text();
			Text val = new Text();
			for (int i = 0; i < keys.length; i++) {
				key.set(keys[i]);
				val.set(values[i] + "");
				context.write(key, val);
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		if (args.length != 2)
			throw new IllegalArgumentException(args.length + " usage: ... ");
		
		String bitvectorpath = args[0], outputPath = args[1];
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(new Cluster(conf), conf);
		job.setJobName("int key replace phase1");
		job.setJarByClass(OutlinkGrowthAnalysis.class);

		job.setMapperClass(BVIdentitiyMapper.class);
		job.setReducerClass(AnaylseOLGrowthReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TabSeperatedTextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileInputFormat.setInputPaths(job, new Path(bitvectorpath));
		
		job.setNumReduceTasks(1);
		job.waitForCompletion(true);

	}

}
