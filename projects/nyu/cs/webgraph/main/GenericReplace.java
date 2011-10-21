package nyu.cs.webgraph.main;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;

import nyu.cs.webgraph.MRhelpers.Commons;
import nyu.cs.webgraph.MRhelpers.CustomFileInputFormats.LinkGraphInputFormat;
import nyu.cs.webgraph.MRhelpers.CustomFileInputFormats.TabSeperatedTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.hadoop.compression.lzo.LzoCodec;
import com.twmacinta.util.MD5;

/*
 * 
 * given a web graph and a mapping, replaces the web graph with values in the mapping 
 * 
 * each line in mapping has the form of <part1 \t part2> and key and value parts should be specified in the mapping
 * each line in webgraph has the form of <node \t stuff_not_to_replace stuff_to_replace> everything is separated by a space and number of not_to_replace items should be specified.
 * 
 */

public class GenericReplace {

	public static final String HASH_SLID = "hashSlinkid"; 
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
	
	public static String sourceIdentifier = "c_";
	public static String destIdentifier = "d_";
	public static String othersIdentifier= "b_";
	public static String mappingIdentifier= "a_";
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
	
	public static class GenerateLinks extends Mapper<Text, Text, Text, Text> {

		Text txtkey = new Text();
		Text txtval = new Text();
		String[] curr_line;
		StringBuffer notToReplacePart;
		StringBuffer currentSuperLinkIdentifier; // includes no single space!
		
		boolean hashSlid;
		MD5 md5; 
		
		@Override
		public void setup(Context context){
			hashSlid = Boolean.parseBoolean(context.getConfiguration().get(HASH_SLID));
			if (hashSlid)
				md5 = Commons.getFastMD5();
		}
		
		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String curr_lineStr = value.toString();
			curr_line = curr_lineStr.split(" ");
			
			if (curr_lineStr.length() != 0 && curr_line.length >= Integer.parseInt(context.getConfiguration().get(NUM_OF_NOT_TO_REPLACE_ITEMS))) 
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
					txtkey.set(curr_line[i]);
					if (hashSlid)
						txtval.set(appendIdentifier(destIdentifier, Commons.hashToStr(md5, currentSuperLinkIdentifier.toString())));
					else
						txtval.set(appendIdentifier(destIdentifier, currentSuperLinkIdentifier));
					context.write(txtkey, txtval);
				}
				//output notToReplace
				if (notToReplacePart.length() > 0) {
					txtkey.set(notToReplacePart.toString());
					if (hashSlid)
						txtval.set(appendIdentifier(othersIdentifier, Commons.hashToStr(md5, currentSuperLinkIdentifier.toString())));
					else
						txtval.set(appendIdentifier(othersIdentifier, currentSuperLinkIdentifier));
					context.write(txtkey, txtval);
				}
				
				//output the source
				if (hashSlid)
					txtval.set(appendIdentifier(sourceIdentifier, Commons.hashToStr(md5, currentSuperLinkIdentifier.toString())));
				else
					txtval.set(appendIdentifier(sourceIdentifier,  currentSuperLinkIdentifier));
				context.write(key, txtval);
			}
		}
	}
	
	public static class GenerateMappings extends Mapper<Text, Text, Text, Text> {
		
		Text txtval = new Text();
		
		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			
			if (Boolean.parseBoolean(context.getConfiguration().get(IS_KEY_FIRST_IN_MAP))) {
				txtval.set(appendIdentifier(mappingIdentifier, value.toString()));
				context.write(key, txtval);
			} else {
				txtval.set(appendIdentifier(mappingIdentifier, key.toString()));
				context.write(value, txtval);
			}
		}
	}
	
	public static class ReplaceReducer extends Reducer<Text, Text, Text, Text> {
		
		String keyToReplace, valMapping, keyNotToReplace;
		String[] bvArr, olArr;

		StringBuffer[] buffArr;
		
		Text txtkey = new Text();
		Text txtvalue = new Text();
		Iterator<Text> valItr;
		
		String current_val;
		String current_idToReplace;
		LinkedList<String> current_destinationSLIDs = new LinkedList<String>();
		LinkedList<String> current_sourceSLIDs= new LinkedList<String>();
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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
						txtkey.set(removeIdentifier(current_val, destIdentifier));
						txtvalue.set(appendIdentifier(destIdentifier, current_idToReplace));
						context.write(txtkey, txtvalue);
					}
					else
						current_destinationSLIDs.add(removeIdentifier(current_val, destIdentifier));
				}
				else if (isSource(current_val)){
					if(current_idToReplace != null){
						txtkey.set(removeIdentifier(current_val, sourceIdentifier));
						txtvalue.set(appendIdentifier(sourceIdentifier, current_idToReplace));
						context.write(txtkey, txtvalue);
					}
					else
						current_sourceSLIDs.add(removeIdentifier(current_val, sourceIdentifier));
				}
				else if (isOthers(current_val)){ // directly output not-to-replace stuff (no replacement required)
					txtkey.set(removeIdentifier(current_val, othersIdentifier));
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
					txtkey.set(current_sourceSLIDs.pop());
					context.write(txtkey, txtvalue);
				}
				//replace each destination
				txtvalue.set(appendIdentifier(destIdentifier, current_idToReplace));
				while (!current_destinationSLIDs.isEmpty()) {
					txtkey.set(current_destinationSLIDs.pop());
					context.write(txtkey, txtvalue);
				}
			}
		}
	}
	
	//SECOND PART - MERGE BACK

	public static class IdenityMapper extends Mapper<Text, Text, Text, Text> {
		
		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class MergeBackReducer extends Reducer<Text, Text, Text, Text> {

		LinkedList<String> destinations  = new LinkedList<String>();
		String source; 
		String others; 
		String curr_val; 
		
		Text txtkey = new Text();
		Text txtvalue = new Text();
		StringBuffer finalval;
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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
				finalval.append(others);
				finalval.append(" ");
				Collections.sort(destinations);
				while (!destinations.isEmpty()){
					finalval.append(destinations.pop());
					finalval.append(" ");
				}
				txtkey.set(source);
				txtvalue.set(finalval.toString());
				context.write(txtkey, txtvalue);
			}
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		String graphPath, mapPath, outputPath, finalOutputPath, samplepath;
		
		if (args.length != 8)
			throw new IllegalArgumentException("arguments: graphPath | mapPath | outputPath | finaloutputPath | isKeyFirstInMapFile | numOfSpaceSeperaredNoReplaceValues | samplepath | hashSlinkId?");
		
		Configuration conf = new Configuration();
		graphPath = args[0];
		mapPath = args[1];
		outputPath = args[2];
		finalOutputPath = args[3];
		conf.set(IS_KEY_FIRST_IN_MAP , args[4]);
		conf.set(NUM_OF_NOT_TO_REPLACE_ITEMS, args[5] );
		samplepath = args[6];
		conf.set(HASH_SLID, args[7]);
		
		System.out.println("hashing slinkid? " + args[7]);
		
		//FIRST PHASE
		//TotalOrderPartitioner.setPartitionFile(conf, new Path(samplepath));
		
		Job job = Job.getInstance(new Cluster(conf), conf);
		System.out.println("parameters:" + Integer.parseInt(job.getConfiguration().get(NUM_OF_NOT_TO_REPLACE_ITEMS)) + " " + Boolean.parseBoolean(job.getConfiguration().get(IS_KEY_FIRST_IN_MAP)));
		job.setJobName("generic replace phase1");
		job.setJarByClass(GenericReplace.class);
		
		MultipleInputs.addInputPath(job, new Path(graphPath), LinkGraphInputFormat.class, GenerateLinks.class);
		MultipleInputs.addInputPath(job, new Path(mapPath), LinkGraphInputFormat.class, GenerateMappings.class);
		job.setReducerClass(ReplaceReducer.class);
		
		//job.setPartitionerClass(TotalOrderPartitioner.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setCompressOutput(job, true);
		TextOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
		
		job.setNumReduceTasks(144);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
		
		//SECOND PHASE
		Job job2 = Job.getInstance(new Cluster(conf), conf);
		job2.setJobName("generic replace phase2");
		job2.setJarByClass(GenericReplace.class);
		
		job2.setReducerClass(MergeBackReducer.class);
		job2.setMapperClass(IdenityMapper.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setInputFormatClass(TabSeperatedTextInputFormat.class);
		
		job2.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setCompressOutput(job2, true);
		TextOutputFormat.setOutputCompressorClass(job2, GzipCodec.class);
		job2.setNumReduceTasks(144);
		
		FileInputFormat.setInputPaths(job2, new Path(outputPath));
		FileOutputFormat.setOutputPath(job2, new Path(finalOutputPath));
		job2.waitForCompletion(true);
	}
}
