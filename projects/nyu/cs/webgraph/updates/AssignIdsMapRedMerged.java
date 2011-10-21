package nyu.cs.webgraph.updates;

import java.io.IOException;
import java.util.HashMap;

import nyu.cs.webgraph.MRhelpers.CustomFileInputFormats.TabSeperatedTextInputFormat;
import nyu.cs.webgraph.main.GetUniqueUrls;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class AssignIdsMapRedMerged {
	
	public static class DummyMapper extends Mapper<Text, Text, Text, Text> {
		
		Text uTaskId = new Text();
		
		@Override
		public void setup(Context context){
			uTaskId.set(context.getTaskAttemptID().getTaskID().getId()+"");
		}
		
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	
	public static class IdAssignerReducer extends Reducer<Text, Text, Text, Text> {
		HashMap<String, Integer> offsets = new HashMap<String, Integer>();
		int currId;
		boolean isFirst;
		Text keyAndOldIds = new Text();
		@Override
		public void setup(Context context) throws IOException{
			currId = 0;
			isFirst = true;
			
			Configuration conf = context.getConfiguration();
			FileSystem fs =  FileSystem.get(conf);
			Text key = new Text(), val = new Text();
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(makeUUrlFileOffsetsPathName(FileInputFormat.getInputPaths(context)[0].toString())),  conf);
			while (reader.next(key, val)){
				offsets.put(key.toString(), Integer.parseInt(val.toString()));
			}
		}
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (isFirst)
			{
				if(offsets.containsKey(key.toString())){
					currId = offsets.get(key.toString());
				}
				else
					throw new IllegalArgumentException("offset value could not be found! " + key.toString());
				isFirst = false;
			}
			
			keyAndOldIds.set(key.toString() + " "+values.iterator().next().toString());
			
			context.write(new Text(currId+""), keyAndOldIds);
			currId++;
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		if (args.length != 3) {
			throw new IllegalArgumentException("Default file paths are in use\nUsage: <uurls_path> <urlidmapping_path> <partitionerSapmle_path>");
		}
		
		String uurls_path = args[0], urlidmap_path = args[1], path_sampleforpartitioner = args[2];
		
		//get first URL of every file partition
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		SequenceFile.Reader reader;
		Text key = new Text(), val = new Text();
		
		HashMap<Integer, String> firtUrlsMap = new HashMap<Integer, String>();
		HashMap<Integer, Integer> countMap = new HashMap<Integer, Integer>();
		String keyIdentifier; //task_201106171320_0079_r_000013
		int currfileid;
		
		System.out.println("counters path  name:" + new Path(GetUniqueUrls.makeUUrlCountersPathName(uurls_path)));
		
		for (FileStatus fstatus : fs.listStatus(new Path(GetUniqueUrls.makeUUrlCountersPathName(uurls_path)))) {
			reader = new Reader(fs, fstatus.getPath(),conf);
			reader.next(key, val);
			keyIdentifier = key.toString();
			currfileid = Integer.parseInt(keyIdentifier.substring(keyIdentifier.lastIndexOf("_")+1));
			countMap.put(currfileid, Integer.parseInt(val.toString()));
			firtUrlsMap.put(currfileid, keyIdentifier.substring(0, keyIdentifier.lastIndexOf("__")));
			//System.out.println(keyIdentifier.substring(0, keyIdentifier.lastIndexOf("__")));
			reader.close();
		}
		
		System.out.println("offsets path name:" + new Path(makeUUrlFileOffsetsPathName(uurls_path)));
		//write down offsets
		SequenceFile.Writer offset_writer = SequenceFile.createWriter(fs, conf, new Path(makeUUrlFileOffsetsPathName(uurls_path)), Text.class, Text.class);
		int current_count = 0;
		for (Integer i : countMap.keySet()) {
			//System.out.println(new Text(firtUrlsMap.get(i)));
			offset_writer.append(new Text(firtUrlsMap.get(i)), new Text(current_count+""));
			current_count += countMap.get(i);
		}
		offset_writer.close(); 
		
		TotalOrderPartitioner.setPartitionFile(conf, new Path(path_sampleforpartitioner));
		Job job = Job.getInstance(new Cluster(conf), conf);
		job.setJobName("assign ids");
		job.setJarByClass(AssignIdsMapRedMerged.class);
		
		job.setMapperClass(DummyMapper.class);
		job.setReducerClass(IdAssignerReducer.class);
		
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TabSeperatedTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(MergeUrlIdMaps.numOfReducers);
		
		FileInputFormat.addInputPath(job, new Path(uurls_path));
		FileOutputFormat.setOutputPath(job, new Path(urlidmap_path));
		
		TextOutputFormat.setCompressOutput(job, true);
		TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
	public static String makeUUrlFileOffsetsPathName(String uurlsPathName){
		return uurlsPathName + "-offsets";
	}
}
