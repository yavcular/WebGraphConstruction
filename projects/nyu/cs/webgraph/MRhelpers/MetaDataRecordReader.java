package nyu.cs.webgraph.MRhelpers;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

/**
 * Treats keys as source URL and value as destination URL (lines indicated with 'l').
 */
public class MetaDataRecordReader extends RecordReader<Text, Text> {
	private static final Log LOG = LogFactory.getLog(MetaDataRecordReader.class);

	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private LineReader in;
	private int maxLineLength;
	private Text key = null;
	private Text value = null;
	
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
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
	
	Text curr_line = new Text();
	int keyendpos;
	byte[] tmpbyte; 
	String curr_str;
	boolean problemline;
	@Override
	public boolean nextKeyValue() throws IOException {
		problemline = false; 
		
		if (key == null) {
			key = new Text();
		}
		if (value == null) {
			value = new Text();
		}
		int newSize = 0;
		if (pos < end) {
			newSize = in.readLine(curr_line);
			if (newSize > 0) {
				pos += newSize;
				curr_str = curr_line.toString();
				
				if (!curr_str.contains(" ")){
					LOG.debug("supposed to be the source line, but there are no space in the line! --" +curr_line+ "--");
					key.set("");
					value.clear();
					problemline = true;
				}
				else
				{
					curr_str = UrlManipulation.normalize(curr_str);
					keyendpos = curr_str.indexOf(" ", 0);
					key.set(curr_str.substring(0, keyendpos));
					
					keyendpos = curr_str.indexOf(" ", keyendpos+1);
					try{
						if (keyendpos < 0)
							System.err.println("keyendpos:" + keyendpos);
						value.set(curr_str.substring(keyendpos+1, curr_str.indexOf(" ", keyendpos+1))); //add time as the first parameter of value
					}catch(Exception e){
						System.err.println("can not get the time for line:" + curr_line.toString());
					}
				}
				
				if (pos < end) //if  there is more to read in the file
				{
					do { 
						curr_line.clear();
						newSize = in.readLine(curr_line);
						pos += newSize;
						curr_str = curr_line.toString();
						if (value.getLength() < 1024*1024*1024) //assuming -xmx1024M value has to be < 1024M
						{
							if (curr_str.startsWith("l ") && !problemline) // if line starts with "l " 
							{
								if (curr_str.contains("\t"))
									tmpbyte = (" "+curr_str.substring(2).replaceAll("\\s+", "")).getBytes();
								else
								{
									curr_str = curr_str.substring(2);
									if (curr_str.contains(" "))
										tmpbyte = (" " + curr_str.replaceAll("\\s+", "")).getBytes();
									else
										tmpbyte = (" " + curr_str).getBytes();
								}
								
								value.append(tmpbyte, 0, tmpbyte.length);
							}
						}
					}
					while (!curr_str.equalsIgnoreCase("") && pos < end);
				}
			}
		}
		if (newSize > 0) {
			return true;
		} else {
			key = null;
			value = null;
			return false;
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
