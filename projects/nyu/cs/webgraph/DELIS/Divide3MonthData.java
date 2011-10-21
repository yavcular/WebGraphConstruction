package nyu.cs.webgraph.DELIS;

import java.io.IOException;

import nyu.cs.webgraph.MRhelpers.CustomFileInputFormats.TabSeperatedTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Divide3MonthData {

	public static String[] first3M = { "200606", "200607", "200608" };
	public static String[] second3M = { "200609", "200610", "200611" };
	public static String[] third3M = { "200612", "200701", "200702" };
	public static String[] forth3M = { "200703", "200704", "200705" };

	public static class IdentitityMapper extends Mapper<Text, Text, Text, Text> {

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class Split3MReducer extends Reducer<Text, Text, Text, Text> {

		private MultipleOutputs mos;

		public void setup(Context context) {
			mos = new MultipleOutputs(context);
		}
		
		String currVal;
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			for (Text val : values) {
				currVal = val.toString().trim();
				if(currVal.startsWith(first3M[0]) || currVal.startsWith(first3M[1]) || currVal.startsWith(first3M[2]))
					mos.write("first3m", key, val);
				else if (currVal.startsWith(second3M[0]) || currVal.startsWith(second3M[1]) || currVal.startsWith(second3M[2]))
					mos.write("second3m", key, val);
				else if (currVal.startsWith(third3M[0]) || currVal.startsWith(third3M[1]) || currVal.startsWith(third3M[2]))
					mos.write("third3m", key, val);
				else if (currVal.startsWith(forth3M[0]) || currVal.startsWith(forth3M[1]) || currVal.startsWith(forth3M[2]))
					mos.write("fourth3m", key, val);
			}
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}

	public static void main(String[] args) throws Exception {
		
		if (args.length != 2) {
			throw new IllegalArgumentException("arguments: inpath, outpath");
		}
		String inpath = args[0];
		String outPath = args[1];

		Configuration conf = new Configuration();

		Job job = new Job(conf, "split 3M");
		job.setJarByClass(Divide3MonthData.class);

		job.setMapperClass(IdentitityMapper.class);
		job.setReducerClass(Split3MReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		MultipleOutputs.addNamedOutput(job, "first3m", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "second3m", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "third3m", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "fourth3m", TextOutputFormat.class, Text.class, Text.class);

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

		job.setInputFormatClass(TabSeperatedTextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inpath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));

		job.setNumReduceTasks(144);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
