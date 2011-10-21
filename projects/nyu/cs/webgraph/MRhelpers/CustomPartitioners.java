package nyu.cs.webgraph.MRhelpers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import nyu.cs.webgraph.archiveUK.CreateLinkGraph;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioners {
	
	private static final Log LOG = LogFactory.getLog(CustomPartitioners.class);
	
	public static class SampleBasedPartitioner extends Partitioner<Text, Writable> {
		private static ArrayList<String> map = null;
		private static Configuration conf = new Configuration();
		
		public static void loadMap() {
			
			LOG.info("loading sample map for partitioner!");
			map = new ArrayList<String>();
			FileSystem fs = null;
			try {
				fs = FileSystem.get(conf);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			BufferedReader boundaryMapReader;
			try {
				boundaryMapReader = new BufferedReader(new InputStreamReader(fs.open(new Path(Commons.sampleBoundriesDir))));

				String line;
				while ((line = boundaryMapReader.readLine()) != null) {
					map.add(line);
				}
				
				boundaryMapReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		public SampleBasedPartitioner(){
			if (map == null)
				loadMap();
		}
		
		@Override
		public int getPartition(Text key, Writable value, int numReduceTasks) {
			/*
			int low_Bound = 0; 
			int high_bound = map.size()-1;
			int next_bound = 0; 
			
			while(next_bound  != ( high_bound + low_Bound ) /2 )
			{
				next_bound = ( high_bound + low_Bound ) /2;
				if (key.toString().compareTo(map.get(next_bound)) < 0)
				{
					high_bound = next_bound;
				}
				else
					low_Bound = next_bound;
			}
			
			if(low_Bound == high_bound && low_Bound == 0)
				return 0 % numReduceTasks;
			else if (high_bound == map.size()-1 && key.toString().compareTo(map.get(high_bound)) > 0) //even larger than largest in the sample
				return map.size() % numReduceTasks;
			else
				return  high_bound % numReduceTasks;*/
			
			int j;
			for (j = 0; j < map.size(); j++) {
				if (key.toString().compareTo(map.get(j)) < 0) // key comes before map[i]
					return j % numReduceTasks;
			}
			return j % numReduceTasks;
		}
	}
	
	public static class BaseDomainPartitioner extends Partitioner<Text, Writable> {
	
		@Override
		public int getPartition(Text key, Writable value, int numReduceTasks) {
			return (UrlManipulation.getBaseDomain(key.toString()).hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}

	
	
	public static class NumericBasePartitioner extends Partitioner<LongWritable, Writable> {

		@Override
		public int getPartition(LongWritable key, Writable value, int numReduceTasks) {
			return (int) (key.get() % numReduceTasks);
		}
	}
	
	public static class NumericIntBasePartitioner extends Partitioner<IntWritable, Writable> {

		@Override
		public int getPartition(IntWritable key, Writable value, int numReduceTasks) {
			return (key.get() % numReduceTasks);
		}
	}
}
