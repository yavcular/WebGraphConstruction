package nyu.cs.webgraph.MRhelpers;

import java.io.IOException;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Treats keys as source URL and value as destination URL (lines indicated with 'l').
 */
public class MultiFileRawWebGraphRecordReader extends RecordReader<Text, Text> {
	private static final Log LOG = LogFactory.getLog(MultiFileRawWebGraphRecordReader.class);

	private CombineFileSplit split;
	private TaskAttemptContext context;
	private int index;
	private RecordReader<Text, Text> rr;

	public MultiFileRawWebGraphRecordReader(CombineFileSplit split,TaskAttemptContext context, Integer index) throws IOException {
		
		this.split = split;
		this.context = context;
		this.index = index;
		rr = new MetaDataRecordReader();
	}
	
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {

		this.split = (CombineFileSplit) genericSplit;
		this.context = context;

		if (null == rr) {
			rr = new MetaDataRecordReader();
		}
		
		FileSplit fileSplit = new FileSplit(this.split.getPath(index), this.split.getOffset(index), this.split.getLength(index), this.split.getLocations());
		this.rr.initialize(fileSplit, this.context);
	}
	
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return rr.nextKeyValue();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return rr.getCurrentKey();
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return rr.getCurrentValue();
	}

	/**
	 * Get the progress within the split
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return rr.getProgress();
	}

	public synchronized void close() throws IOException {
		if (rr != null) {
			rr.close();
			rr = null;
		}
	}
	
}
