package nyu.cs.webgraph.archiveUK;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import nyu.cs.webgraph.MRhelpers.Commons;
import nyu.cs.webgraph.MRhelpers.UrlManipulation;
import nyu.cs.webgraph.MRhelpers.CustomFileInputFormats.RawToLinkInputFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;


/**
 * This MR job converts raw web graph to following format
 * "sourceURL destURL destURL ..."
 */
public class CreateLinkGraph extends Configured implements Tool {
	
	private static final Log LOG = LogFactory.getLog(CreateLinkGraph.class);
	
	public static class BaseDomainPartitionerNuLL extends Partitioner<NullWritable, Text> {
		
		int spacepos;
		String valline;
		@Override
		public int getPartition(NullWritable key, Text value, int numReduceTasks) {
			valline = value.toString();
			if ((spacepos = valline.indexOf(" ")) == -1)
				return (UrlManipulation.getBaseDomain(valline).hashCode() & Integer.MAX_VALUE) % numReduceTasks;
			else
				return (UrlManipulation.getBaseDomain(valline.substring(0, spacepos)).hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}
	
	public static class GMapper extends Mapper<Text, Text, NullWritable, Text> {
		
		NullWritable nw = NullWritable.get();
		Text link = new Text();

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			
			if (key.find("filedesc") != 0) // if !key.startswith(filedesc)
			{
				link.set(key.toString() + "\t" + value.toString());
				context.write(nw, link);
			}
		}
	}
	
	public static class GMapperNoreducer extends Mapper<Text, Text, Text, Text> {
		
		MultipleOutputs mos; 
		Text nullTxt = new Text("");
		public static ArrayList<String> sample;
		
		public void setup(Context context) {
			mos = new MultipleOutputs(context);
		}
		
		Random random = new Random();
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			
			if (key.find("filedesc") != 0) // if !key.startswith(filedesc)
			{
				mos.write("output",key, value);
				
				// sampling
				if (random.nextDouble() <= Commons.sampleFrequency)//*value.toString().split(" ").length) 
					mos.write("sample", key, nullTxt, Commons.sampleDir + "sample");
				for (String outlink : value.toString().split(" ")) {
					if (random.nextDouble() <= Commons.sampleFrequency) 
						mos.write("sample", key, nullTxt, Commons.sampleDir + "sample");
				}
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			mos.close();
		}
	}
	
	public static class GReducer extends Reducer<NullWritable, Text, Text, Text> {
		String[] keyval;
		Text newkey = new Text();
		Text newval = new Text();
		String curr_val;
		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				curr_val = val.toString();
				if(curr_val.contains("\t"))
				{
					keyval = curr_val.split("\t");
					if (keyval.length == 2){
						newkey.set(keyval[0]);
						newval.set(keyval[1]);
						context.write(newkey, newval);
					}
					else
					{
						if (keyval.length > 0)
						LOG.error("multiple tabs! line:"+ val);
					}
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		System.err.println("raw graph to link graph - hadi amaaaa");
		System.err.println("sample frequency:" + Double.toString(Commons.sampleFrequency)+"\n");
		
		boolean hasReduce = false;
		
		boolean isDefaultFilePath = false;
		Configuration conf = new Configuration();
		FileSystem.get(conf).delete(new Path(Commons.sampleDir), true);
		FileSystem.get(conf).delete(new Path(Commons.sampleMergedDir), true);
		FileSystem.get(conf).delete(new Path(Commons.sampleBoundriesDir), true);
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Default file paths are in use\nUsage: RawGraphToLinkGraph <in> <out>");
			isDefaultFilePath = true;
		}
		Job job =  Job.getInstance(new Cluster(conf), conf);
		job.setJobName("extract link graph out of metadata");
		
		job.setJarByClass(CreateLinkGraph.class);
		
		job.setInputFormatClass(RawToLinkInputFormat.class);
		
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		
		if (hasReduce){
			System.err.println("nullwritable with reducers");
			job.setReducerClass(GReducer.class);
			job.setMapperClass(GMapper.class);
			job.setMapOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			//job.setPartitionerClass(BaseDomainPartitioner.class);
			job.setPartitionerClass(BaseDomainPartitionerNuLL.class);
			job.setNumReduceTasks(40); //0.95 or 1.75 * (nodes (11 beaker nodes) * mapred.tasktracker.tasks.maximum (default 2) )
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
		}
		else
		{
			System.err.println("no reducers!");
			job.setMapperClass(GMapperNoreducer.class);
			MultipleOutputs.addNamedOutput(job, "output", SequenceFileOutputFormat.class , Text.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "sample", SequenceFileOutputFormat.class , Text.class, Text.class);
			job.setNumReduceTasks(0);
		}
		
		if (isDefaultFilePath){
			FileInputFormat.addInputPath(job, new Path(Commons.METADATA_PATH));
			FileOutputFormat.setOutputPath(job, new Path(Commons.LINKGRAPH_PATH));
			System.err.println("input/output paths" + Commons.METADATA_PATH + " & " + Commons.LINKGRAPH_PATH );
		}
		else
		{
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			System.err.println("input/output paths" + new Path(otherArgs[0]) + " & " + new Path(otherArgs[1]) );
		}
		
		boolean isSuccess = job.waitForCompletion(true);
		
		//prepareSampleMap(conf);
		
		System.exit( isSuccess ? 0 : 1);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public static void prepareSampleMap(Configuration conf) throws IOException{
		
		LOG.info("creating sample ..");
		System.err.println("writing out the sample files");
		
		FileSystem fs = FileSystem.get(conf);
		BufferedWriter writerMerged = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(Commons.sampleMergedDir))));
		BufferedWriter writerBoundries = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(Commons.sampleBoundriesDir))));
		SequenceFile.Reader reader;
		
		ArrayList<String> urls = new ArrayList<String>();
		
		Text key = new Text();
		Text val = new Text();
		for (FileStatus currInFile : fs.listStatus(new Path(Commons.sampleDir))) {
			reader = new SequenceFile.Reader(fs, currInFile.getPath(), conf);
			while (reader.next(key, val)) {
				urls.add(key.toString());
			}
			reader.close();
		}
		
		Collections.sort(urls);
		
		for (int i = 0; i < urls.size(); i++) {
			writerMerged.append(urls.get(i));
			writerMerged.append(System.getProperty("line.separator"));
			
			if (i % Commons.fudgeFactor == 0)
			{
				
				writerBoundries.append(urls.get(i));
				writerBoundries.append(System.getProperty("line.separator"));
			}
		}
		/*writerBoundries.append( urls.get(urls.size()-1));
		writerBoundries.append(System.getProperty("line.separator"));
		*/
		writerMerged.close();
		writerBoundries.close();
		
	}	
}
