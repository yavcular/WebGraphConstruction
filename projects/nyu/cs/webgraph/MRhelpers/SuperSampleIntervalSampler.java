package nyu.cs.webgraph.MRhelpers;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import nyu.cs.webgraph.main.GenerateSuperSample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.Sampler;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Requires sorted files, and total number of lines should be known
 * 
 */
public class SuperSampleIntervalSampler {
	private int numOfREducers;
	private int sampleSize;
	private Job job;
	private FileSystem fs;
	private Configuration conf;
	
	public SuperSampleIntervalSampler(){
		
	}
	
	public SuperSampleIntervalSampler(Job job) throws IOException, ClassNotFoundException {
		this.numOfREducers = job.getNumReduceTasks();
		this.conf = job.getConfiguration();
		this.fs = FileSystem.get(conf);
		this.job = job;
		
		//get sample count
		FSDataInputStream dis = fs.open(new Path ( GenerateSuperSample.makeSSampleCounterPathName(FileInputFormat.getInputPaths(job)[0].toString()))); 
        sampleSize = dis.readInt(); 
        dis.close(); 
	}

	public ArrayList<Text> getSample() throws IOException, InterruptedException, ClassNotFoundException {
		
		final InputFormat inf = ReflectionUtils.newInstance(job.getInputFormatClass(), conf);
		List<InputSplit> splits = inf.getSplits(job);
		ArrayList<Text> samples = new ArrayList<Text>();
		int splitsToSample = splits.size();
		int splitStep = splits.size() / splitsToSample;
		
		for (int i = 0; i < splitsToSample; ++i) {
			RecordReader reader = inf.createRecordReader(splits.get(i * splitStep), new TaskAttemptContextImpl(job.getConfiguration(),new TaskAttemptID()));
			
			int stepSize = sampleSize / (numOfREducers);
			int lineCouner = 0;
			System.out.println("sample step size:" + stepSize);
			
			while (reader.nextKeyValue()) {
				lineCouner++;
				if (lineCouner % stepSize == 0) {
					//System.out.println("current line counter" + lineCouner + " " + reader.getCurrentKey().toString());
					samples.add(new Text(reader.getCurrentKey().toString()));
				}
			}
			reader.close();
		}
		System.out.println("sampling completed.");
		return samples;
	}
	
	public void writePartitioner(ArrayList<Text> samples) throws IOException{
		
		if (fs.exists( new Path(TotalOrderPartitioner.getPartitionFile(conf)))) {
			fs.delete(new Path(TotalOrderPartitioner.getPartitionFile(conf)), false);
		}
		
		SequenceFile.Writer writer = SequenceFile.createWriter(FileSystem.get(conf), conf, new Path(TotalOrderPartitioner.getPartitionFile(job.getConfiguration())), job.getMapOutputKeyClass(), NullWritable.class);
		for (int i = 0; i < samples.size()-1; i++) {
			writer.append(samples.get(i), NullWritable.get());
		}
		writer.close();
		System.out.println("sampling flushed to disk.");
	}
	
}
