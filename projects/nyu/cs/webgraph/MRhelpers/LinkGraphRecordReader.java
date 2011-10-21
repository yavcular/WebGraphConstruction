package nyu.cs.webgraph.MRhelpers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/**
 * Reads line by line.
 * splits every line into two by the FIRST \t occurance, 
 * sets the key as first half value as second half. 
 * 
 * if there is no \t then sets the line as key and value is empty
 */
public class LinkGraphRecordReader extends RecordReader<Text, Text> {
	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private LineReader in;
	private int maxLineLength;
	private Text key = null;
	private Text value = null;

	private static final Log LOG = LogFactory.getLog(LinkGraphRecordReader.class);
	
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();
		compressionCodecs = new CompressionCodecFactory(job);
		final CompressionCodec codec = compressionCodecs.getCodec(file);

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		boolean skipFirstLine = false;
		if (codec != null) {
			in = new LineReader(codec.createInputStream(fileIn), job);
			end = Long.MAX_VALUE;
		} else {
			if (start != 0) {
				skipFirstLine = true;
				--start;
				fileIn.seek(start);
			}
			in = new LineReader(fileIn, job);
		}
		if (skipFirstLine) { // skip first line and re-establish "start".
			start += in.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
		}
		this.pos = start;
	}
	
	String currentline;
	String[] currentlinearr;
	Text line = new Text();
	int newSize;
	public boolean nextKeyValue() throws IOException {
		if (key == null) {
			key = new Text();
		}
		if (value == null) {
			value = new Text();
		}
		newSize = 0;
		if (pos < end) {
			newSize = in.readLine(line);
			if (newSize != 0) {
				pos += newSize;
				
				currentline = line.toString();
				if (currentline.contains("\t"))
				{
					currentlinearr = currentline.split("\t");
					if (currentlinearr.length != 2)
						LOG.error("pirrrrrrrR " + currentline);
					else
					{
						key.set(currentlinearr[0]);
						value.set(currentlinearr[1]);
					}
				}
				else
				{
					key.set(line);
					value.set("");
				}
				
			/*	currentlinearr = line.toString().split("\t");
				
				if (currentlinearr.length > 1)
				{
					key.set(currentlinearr[0]);
					value.set(currentlinearr[1]);
					
					currentline = line.toString();
					if (currentlinearr.length > 2)
						LOG.debug("arraylength is big!" + currentlinearr.length);
					if (!currentline.substring(0, tabpos).equalsIgnoreCase(currentlinearr[0]))
						LOG.debug("keys does not match!" + "--" + currentline.substring(0, tabpos) + "--" + currentlinearr[0], new IOException());
					if (!currentline.substring(tabpos+1).equalsIgnoreCase(currentlinearr[1]))
						LOG.debug("values does not match!" + "--" + currentline.substring(tabpos+1) + "--" + currentlinearr[1], new IOException());
				}
				else
				{
					key.set(currentlinearr[0]);
					value.set("");
				}*/
			}
		}
		if (newSize == 0) {
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}
	}

	@Override
	public Text getCurrentKey() {
		return key;
	}

	@Override
	public Text getCurrentValue() {
		return value;
	}


	/**
	 * Get the progress within the split
	 */
	public float getProgress() {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	public synchronized void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}
}
