package nyu.cs.webgraph.main;

import java.io.IOException;

import nyu.cs.webgraph.MRhelpers.SuperSampleIntervalSampler;
import nyu.cs.webgraph.MRhelpers.CustomFileInputFormats.TabSeperatedTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class Sample {

	
	public static void sampleForTotalOrderPatitioner(String superSamplePath, String samplePath, int numOfReducers) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration sampler_conf = new Configuration();
		TotalOrderPartitioner.setPartitionFile(sampler_conf, new Path(samplePath));
		Job sampler_job = Job.getInstance(new Cluster(sampler_conf), sampler_conf);
		sampler_job.setJobName("to get partitioner sample from super sample");
		sampler_job.setInputFormatClass(TabSeperatedTextInputFormat.class);
		sampler_job.setMapOutputKeyClass(Text.class);
		sampler_job.setNumReduceTasks(numOfReducers);
		SequenceFileInputFormat.addInputPath(sampler_job, new Path(superSamplePath));
		SuperSampleIntervalSampler sampler = new SuperSampleIntervalSampler(sampler_job);
		sampler.writePartitioner(sampler.getSample());
	}
	
	public static int writeNumericSampleForTOP(String offsetsPath, String numericSamplePath, Configuration conf, Job job) throws IOException, ClassNotFoundException, InterruptedException{
		int count_offsets = 0; 
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path( numericSamplePath))) {
			fs.delete(new Path(numericSamplePath), false);
		}
		SequenceFile.Reader reader = new Reader(fs, new Path(offsetsPath),conf);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(numericSamplePath), job.getMapOutputKeyClass(), NullWritable.class);
		
		Text offset_key = new Text(), offset_val = new Text(); 
		reader.next(offset_key, offset_val); count_offsets++;//skip the first one, it is 0
		LongWritable lw = new LongWritable();
		while(reader.next(offset_key, offset_val)){
			count_offsets++;
			lw.set(Long.parseLong(offset_val.toString()));
			writer.append(lw, NullWritable.get());
		}
		writer.close();
		reader.close();
		
		return count_offsets;
	}
	
}
