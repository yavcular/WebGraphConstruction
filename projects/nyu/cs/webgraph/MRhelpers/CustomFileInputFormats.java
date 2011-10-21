package nyu.cs.webgraph.MRhelpers;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class CustomFileInputFormats {

	
	public static class RawToLinkInputFormat extends CombineFileInputFormat<Text, Text> {
		@Override
		public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
			return new CombineFileRecordReader<Text, Text>((CombineFileSplit) split, context, MultiFileRawWebGraphRecordReader.class);
		}

		@Override
		protected boolean isSplitable(JobContext context, Path file) {
			CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
			return codec == null;
		}
	}

	/*
	 * reads line by line
	 * for each line, separate key and value with tab.
	 * expects <Long, text> pair
	 * more details @IdUrlMapRecordReader
	 */
	public static class IdUrlMapInputFormat extends FileInputFormat<LongWritable, Text> {

		@Override
		public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			RecordReader<LongWritable, Text> rr = new IdUrlMapRecordReader();
			rr.initialize(split, context);
			return rr;
		}

		@Override
		protected boolean isSplitable(JobContext context, Path file) {
			CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
			return codec == null;
		}
	}
	
	/*
	 * reads line by line
	 * for each line, separate key and value with tab.
	 * expects <text, text> pair
	 * more details: @LinkGraphRecordReader
	 * requires line to have at least two values separated by tab
	 */
	public static class LinkGraphInputFormat extends FileInputFormat<Text, Text> {

		@Override
		public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			RecordReader<Text, Text> rr = new LinkGraphRecordReader();
			rr.initialize(split, context);
			return rr;
		}

		@Override
		protected boolean isSplitable(JobContext context, Path file) {
			CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
			return codec == null;
		}
	}
	
	/*
	 * reads line by line
	 * for each line, separate key and value with tab.
	 * expects <text, text> pair
	 * more details: @TabSeparatedLineRecordReader
	 */
	public static class TabSeperatedTextInputFormat extends FileInputFormat<Text, Text> {

		@Override
		public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			RecordReader<Text, Text> rr = new TabSeparatedLineRecordReader();
			rr.initialize(split, context);
			return rr;
		}

		@Override
		protected boolean isSplitable(JobContext context, Path file) {
			CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
			return codec == null;
		}
	}

}
