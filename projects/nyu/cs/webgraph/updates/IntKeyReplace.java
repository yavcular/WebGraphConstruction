package nyu.cs.webgraph.updates;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;

import nyu.cs.webgraph.MRhelpers.CustomFileInputFormats.TabSeperatedTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import com.hadoop.compression.lzo.LzoCodec;

public class IntKeyReplace {

		public static final String NUM_OF_NOT_TO_REPLACE_ITEMS = "numOfSpaceSeperaredNoReplaceValues" ;
		public static final String IS_KEY_FIRST_IN_MAP = "IsKeyFirstInMapFile";
		
		/*
		 * 
		 * FIRST PHASE
		 * 
		 * Map outputs:
		 * 
		 * from graph of line "source non-replace-stuff replace-stuff"; outputs
		 * <src, s slid> 
		 * <norep, x slid> 
		 * <dest, d slid>
		 * 
		 * from url-id mapping for line "entr1 enry2"; outputs 
		 * <key, m value>
		 * 
		 * Reduce replaces:
		 * <slid, s value>
		 * <slid, d value>
		 * <slid, x norep>
		 * 
		 * SECOND PHASE
		 * 
		 * Identity Map, in the form of  
		 * <slid, s|d|x value>
		 * 
		 * Merge Reducer
		 * <s, norep dest dest dest>
		 * 
		 */
		
		public static String sourceIdentifier = "s_";
		public static String destIdentifier = "d_";
		public static String othersIdentifier= "o_";
		public static String mappingIdentifier= "m_";
		public static int valIdentifierLength = destIdentifier.length();
		
		public static String appendIdentifier(String identifier, StringBuffer str) {
			return new StringBuffer(identifier).append(str).toString();
		}
		public static String appendIdentifier(String identifier, String str) {
			return new StringBuffer(identifier).append(str).toString();
		}
		public static StringBuffer appendToSLinkIdentifier(StringBuffer base, String toAppend) {
			return base.append("-").append(toAppend);
		}
		
		public static boolean isMapping(String str) {
			return str.startsWith(mappingIdentifier) ? true : false;
		}
		public static boolean isSource(String str) {
			return (str.startsWith(sourceIdentifier)) ? true : false;
		}
		public static boolean isDest(String str) {
			return (str.startsWith(destIdentifier)) ? true : false;
		}
		public static boolean isOthers(String str) {
			return (str.startsWith(othersIdentifier)) ? true : false;
		}
		
		public static String removeIdentifier(String str, String identifier) {
			return str.substring(identifier.length());
		}
		
		//phsaetwo
		public static String getSource(String str) {
			return str.substring(valIdentifierLength);
		}
		public static String getDest(String str) {
			return str.substring(valIdentifierLength);
		}
		public static String getNoReplStuff(String str) {
			return str.substring(valIdentifierLength);
		}
		
		public static class GenerateLinks extends Mapper<Text, Text, LongWritable, Text> {

			LongWritable txtkey = new LongWritable();
			Text txtval = new Text();
			String[] curr_line;
			StringBuffer notToReplacePart;
			StringBuffer currentSuperLinkIdentifier; // includes no single space!
			
			@Override
			public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
				String curr_lineStr = value.toString();
				curr_line = curr_lineStr.split(" ");
				
				//curr_lineStr.length() != 0 &&
				if ( curr_line.length >= Integer.parseInt(context.getConfiguration().get(NUM_OF_NOT_TO_REPLACE_ITEMS))) 
				{
					currentSuperLinkIdentifier = new StringBuffer(key.toString());
					notToReplacePart = new StringBuffer();
					
					int i = 0;
					for (; i < Integer.parseInt(context.getConfiguration().get(NUM_OF_NOT_TO_REPLACE_ITEMS)); i++) {
						notToReplacePart.append(curr_line[i] + " ");
						appendToSLinkIdentifier(currentSuperLinkIdentifier, curr_line[i]);
					}
					if (currentSuperLinkIdentifier.length() == 0)
						throw new IOException("SLinkIdentifier is empty! <" + key.toString() + "," + value.toString() + ">" + "array.length" + curr_line.length + "firstarg"+ curr_line[0] + "-");
					
					//output destinations
					for (; i < curr_line.length; i++) {
						if (curr_line[i].length() > 0){
							txtkey.set(Long.parseLong(curr_line[i]));
							txtval.set(appendIdentifier(destIdentifier, currentSuperLinkIdentifier));
							context.write(txtkey, txtval);
						}
					}
					//output notToReplace
					if (notToReplacePart.length() > 0) {
						txtkey.set(Long.parseLong(notToReplacePart.toString()));
							txtval.set(appendIdentifier(othersIdentifier, currentSuperLinkIdentifier));
						context.write(txtkey, txtval);
					}
					
					//output the source
					txtkey.set(Long.parseLong(key.toString()));
					txtval.set(appendIdentifier(sourceIdentifier,  currentSuperLinkIdentifier));
					context.write(txtkey, txtval);
				}
				
			}
		}
		
		public static class GenerateMappings extends Mapper<Text, Text, LongWritable, Text> {
			
			Text txtval = new Text();
			LongWritable lkey = new LongWritable();
			
			@Override
			public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
				
				if (Boolean.parseBoolean(context.getConfiguration().get(IS_KEY_FIRST_IN_MAP))) {
					lkey.set(Long.parseLong(key.toString()));
					txtval.set(appendIdentifier(mappingIdentifier, value.toString()));
					context.write(lkey, txtval);
				} else {
					lkey.set(Long.parseLong(value.toString()));
					txtval.set(appendIdentifier(mappingIdentifier, key.toString()));
					context.write(lkey, txtval);
				}
			}
		}
		
		public static class ReplaceReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
			
			String keyToReplace, valMapping, keyNotToReplace;
			String[] bvArr, olArr;

			StringBuffer[] buffArr;
			
			LongWritable txtkey = new LongWritable();
			Text txtvalue = new Text();
			Iterator<Text> valItr;
			
			String current_val;
			String current_idToReplace;
			LinkedList<String> current_destinationSLIDs = new LinkedList<String>();
			LinkedList<String> current_sourceSLIDs= new LinkedList<String>();
			@Override
			public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				current_idToReplace = null;
				current_destinationSLIDs.clear();
				current_sourceSLIDs.clear();
				//System.gc();
				
				valItr = values.iterator();
				
				while (valItr.hasNext())
				{
					current_val = null;
					current_val  = valItr.next().toString();
					if (isMapping(current_val)){ // it is a to-replace key
						current_idToReplace = removeIdentifier(current_val, mappingIdentifier);
					}
					else if (isDest(current_val)){
						if(current_idToReplace != null){
							txtkey.set(Long.parseLong(removeIdentifier(current_val, destIdentifier)));
							txtvalue.set(appendIdentifier(destIdentifier, current_idToReplace));
							context.write(txtkey, txtvalue);
						}
						else
							current_destinationSLIDs.add(removeIdentifier(current_val, destIdentifier));
					}
					else if (isSource(current_val)){
						if(current_idToReplace != null){
							txtkey.set(Long.parseLong(removeIdentifier(current_val, sourceIdentifier)));
							txtvalue.set(appendIdentifier(sourceIdentifier, current_idToReplace));
							context.write(txtkey, txtvalue);
						}
						else
							current_sourceSLIDs.add(removeIdentifier(current_val, sourceIdentifier));
					}
					else if (isOthers(current_val)){ // directly output not-to-replace stuff (no replacement required)
						txtkey.set(Long.parseLong(removeIdentifier(current_val, othersIdentifier)));
						txtvalue.set(appendIdentifier(othersIdentifier, key.toString()));
						context.write(txtkey,txtvalue);
					}
					else
						throw new IllegalArgumentException("WG: Unknown Identfier!");
				}
				if (current_idToReplace != null)
				{
					//replace each source
					txtvalue.set(appendIdentifier(sourceIdentifier, current_idToReplace));
					while (!current_sourceSLIDs.isEmpty()) 
					{
						txtkey.set(Long.parseLong(current_sourceSLIDs.pop()));
						context.write(txtkey, txtvalue);
					}
					//replace each destination
					txtvalue.set(appendIdentifier(destIdentifier, current_idToReplace));
					while (!current_destinationSLIDs.isEmpty()) {
						txtkey.set(Long.parseLong(current_destinationSLIDs.pop()));
						context.write(txtkey, txtvalue);
					}
				}
			}
		}
		
		//SECOND PART - MERGE BACK
		
		public static class IdenityMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
			
			@Override
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
				context.write(key, value);
			}
		}
		
		public static class MergeBackReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

			LinkedList<String> destinations  = new LinkedList<String>();
			String source; 
			String others; 
			String curr_val; 
			
			LongWritable txtkey = new LongWritable();
			Text txtvalue = new Text();
			StringBuffer finalval;
			
			@Override
			public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				destinations.clear();
				Iterator<Text> itr = values.iterator();
				finalval = new StringBuffer();
				
				while (itr.hasNext())
				{
					curr_val = itr.next().toString();
					if (isSource(curr_val))
						source = getSource(curr_val);
					else if (isDest(curr_val))
						destinations.add(getDest(curr_val));
					else if (isOthers(curr_val))
						others = getNoReplStuff(curr_val);
				}
				if (source != null)
				{
					if (others != null)
						finalval.append(others).append(" ");
					
					Collections.sort(destinations);
					while (!destinations.isEmpty()){
						finalval.append(destinations.pop()).append(" ");
					}
					txtkey.set(Long.parseLong(source));
					txtvalue.set(finalval.toString());
					context.write(txtkey, txtvalue);
				}
			}
		}
		
		public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
			
			String graphPath, mapPath, outputPath, finalOutputPath, numericsamplepath;
			
			if (args.length != 7)
				throw new IllegalArgumentException("incorrect number of arguments(" + args.length+ "): graphPath | mapPath | outputPath | finaloutputPath | isKeyFirstInMapFile | numOfSpaceSeperaredNoReplaceValues | samplepath");
			
			Configuration conf = new Configuration();
			graphPath = args[0];
			conf.set(NUM_OF_NOT_TO_REPLACE_ITEMS, args[1] );
			mapPath = args[2];
			conf.set(IS_KEY_FIRST_IN_MAP , args[3]);
			outputPath = args[4];
			finalOutputPath = args[5];
			numericsamplepath = args[6];
			
			//FIRST PHASE
			TotalOrderPartitioner.setPartitionFile(conf, new Path(numericsamplepath));
			
			Job job = Job.getInstance(new Cluster(conf), conf);
			System.out.println("parameters:" + Integer.parseInt(job.getConfiguration().get(NUM_OF_NOT_TO_REPLACE_ITEMS)) + " " + Boolean.parseBoolean(job.getConfiguration().get(IS_KEY_FIRST_IN_MAP)));
			job.setJobName("int key replace phase1");
			job.setJarByClass(IntKeyReplace.class);
			
			MultipleInputs.addInputPath(job, new Path(graphPath), TabSeperatedTextInputFormat.class, GenerateLinks.class);
			MultipleInputs.addInputPath(job, new Path(mapPath), TabSeperatedTextInputFormat.class, GenerateMappings.class);
			job.setReducerClass(ReplaceReducer.class);
			
			job.setPartitionerClass(TotalOrderPartitioner.class);
			
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			TextOutputFormat.setCompressOutput(job, true);
			TextOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
			
			job.setNumReduceTasks(MergeUrlIdMaps.numOfReducers);
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			
			job.waitForCompletion(true);
			
			//SECOND PHASE
			Job job2 = Job.getInstance(new Cluster(conf), conf);
			job2.setJobName("int key replace phase2");
			job2.setJarByClass(IntKeyReplace.class);
			job2.setPartitionerClass(TotalOrderPartitioner.class);
			
			job2.setReducerClass(MergeBackReducer.class);
			job2.setMapperClass(IdenityMapper.class);
			
			job2.setMapOutputKeyClass(LongWritable.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setOutputKeyClass(LongWritable.class);
			job2.setOutputValueClass(Text.class);
			
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			
			job2.setOutputFormatClass(SequenceFileOutputFormat.class);
			SequenceFileOutputFormat.setCompressOutput(job2, true);
			SequenceFileOutputFormat.setOutputCompressorClass(job2, GzipCodec.class);
			job2.setNumReduceTasks(MergeUrlIdMaps.numOfReducers);
			
			FileInputFormat.setInputPaths(job2, new Path(outputPath));
			FileOutputFormat.setOutputPath(job2, new Path(finalOutputPath));
			job2.waitForCompletion(true);
		}
	
}
