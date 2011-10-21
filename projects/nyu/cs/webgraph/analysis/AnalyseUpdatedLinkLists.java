package nyu.cs.webgraph.analysis;

import java.io.IOException;
import java.util.ArrayList;

import nyu.cs.webgraph.MRhelpers.CustomFileInputFormats.TabSeperatedTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AnalyseUpdatedLinkLists {

	public static String OL_INDENTIFIER = "ol_";
	public static String BV_INDENTIFIER = "bv_";

	public static String appendBVIdentifier(String str) {
		return BV_INDENTIFIER + str;
	}

	public static String appendOLIdentifier(String str) {
		return OL_INDENTIFIER + str;
	}

	public static boolean isOL(String str) {
		return str.startsWith(OL_INDENTIFIER);
	}

	public static boolean isBV(String str) {
		return str.startsWith(BV_INDENTIFIER);
	}

	public static String removeIdentifier(String str, String identifier) {
		return str.substring(identifier.length());
	}

	public static ArrayList<String> toArrayList(String[] arr) {
		ArrayList<String> al = new ArrayList<String>();
		for (int i = 0; i < arr.length; i++) {
			al.add(arr[i]);
		}
		return al;
	}

	public static class OLForMergeMapper extends Mapper<Text, Text, Text, Text> {
		Text val = new Text();

		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			val.set(appendOLIdentifier(value.toString()));
			context.write(key, val);
		}
	}

	public static class BVForMergeMapper extends Mapper<Text, Text, Text, Text> {
		Text val = new Text();

		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			val.set(appendBVIdentifier(value.toString()));
			context.write(key, val);
		}
	}

	public static class ExtractAddedRemovedLinksReducer extends Reducer<Text, Text, Text, Text> {

		private MultipleOutputs mos;

		public void setup(Context context) {
			mos = new MultipleOutputs(context);
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String[] bvArr, olArr;
			String currBV, nextBV;
			String curr_val, curr_bvline = null, curr_olline = null;
			StringBuffer addedLinksList = new StringBuffer();
			StringBuffer removedLinksList = new StringBuffer();

			for (Text text : values) {
				curr_val = text.toString();
				if (isBV(curr_val))
					curr_bvline = removeIdentifier(curr_val, BV_INDENTIFIER);
				else if (isOL(curr_val))
					curr_olline = removeIdentifier(curr_val, OL_INDENTIFIER);
			}

			if (curr_olline == null || curr_bvline == null)
				return;

			bvArr = curr_bvline.split(" ");
			if (bvArr.length < 2)
				return;
			
			olArr = curr_olline.split(" ");
			if (bvArr[0].length() != olArr.length)
				throw new IllegalArgumentException("bv length(" + bvArr[0].length() + ") and numer of ols(" + olArr.length + ") dies not match!");

			for (int i = 0; i < bvArr.length - 1; i++) {
				currBV = bvArr[i];
				nextBV = bvArr[i + 1];

				if (nextBV.length() != olArr.length)
					throw new IllegalArgumentException("bv length(" + nextBV.length() + ") and numer of ols(" + olArr.length + ") dies not match!");

				for (int j = 0; j < currBV.length(); j++) {
					if (currBV.charAt(j) != nextBV.charAt(j)) {
						if (currBV.charAt(j) == '1') {
							removedLinksList.append(olArr[j] + " ");
						} else if (currBV.charAt(j) == '0') {
							addedLinksList.append(olArr[j] + " ");
						}
					}
				}
			}
			if (removedLinksList.length() != 0)
				mos.write("removed", key, new Text(removedLinksList.toString()));
			if (addedLinksList.length() != 0)
				mos.write("added", key, new Text(addedLinksList.toString()));
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}
	
	public static void main(String[] args) throws Exception {

		String bvpath, olpath, outpath;
		if (args.length != 3) {
			throw new IllegalArgumentException("arguments: bvpath, olpath, outpath");
		}
		
		bvpath = args[0];
		olpath = args[1];
		outpath = args[2];

		Configuration conf = new Configuration();
		Job job = new Job(conf, "output added removed link lists");
		job.setJarByClass(AnalyseUpdatedLinkLists.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path(bvpath), TabSeperatedTextInputFormat.class , BVForMergeMapper.class);
		MultipleInputs.addInputPath(job, new Path(olpath), TabSeperatedTextInputFormat.class, OLForMergeMapper.class);
		
		MultipleOutputs.addNamedOutput(job, "removed", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "added", TextOutputFormat.class, Text.class, Text.class);
		job.setReducerClass(ExtractAddedRemovedLinksReducer.class);

		FileOutputFormat.setOutputPath(job, new Path(outpath));

		// generate a numeric sampling using offsets for partitioning
		job.setNumReduceTasks(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
