package nyu.cs.webgraph.main;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;

import nyu.cs.webgraph.MRhelpers.Commons;
import nyu.cs.webgraph.MRhelpers.CustomFileInputFormats.IdUrlMapInputFormat;
import nyu.cs.webgraph.MRhelpers.CustomPartitioners.BaseDomainPartitioner;
import nyu.cs.webgraph.MRhelpers.CustomPartitioners.SampleBasedPartitioner;
import nyu.cs.webgraph.structures.LinkAttr;
import nyu.cs.webgraph.structures.LinkAttrOrUrlMapping;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
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
import org.apache.hadoop.util.GenericOptionsParser;

public class Replace {
	
	/*//for local writers
	public static class CombineOutputFormat extends TextOutputFormat<BytesWritable, LinkAttr> {
		@Override
		public RecordWriter<BytesWritable, LinkAttr>  getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
			Configuration conf = job.getConfiguration();
			boolean isCompressed = getCompressOutput(job);
			String keyValueSeparator = conf.get(SEPERATOR, "\t");
			CompressionCodec codec = null;
			String extension = "";
			if (isCompressed) {
				Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
				codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
				extension = codec.getDefaultExtension();
			}
			Path regular_outputpath = FileOutputFormat.getOutputPath(job);
			//Path noSort_outputpath = new Path(regular_outputpath.getParent(), regular_outputpath.getName() + "-nosort");
			//Path file = new Path(noSort_outputpath, job.getTaskAttemptID().toString());
			Path file = new Path(regular_outputpath, job.getTaskAttemptID().toString() + "-l");
			FileSystem fs = file.getFileSystem(conf);
			FSDataOutputStream fileOut;
			if (fs.exists(file) && fs.isFile(file))
				fileOut = fs.append(file);
			else
				fileOut = fs.create(file, false);
			if (!isCompressed) {
				return new LineRecordWriter<BytesWritable, LinkAttr> (fileOut, keyValueSeparator);
			} else {
				return new LineRecordWriter<BytesWritable, LinkAttr>(new DataOutputStream(codec.createOutputStream(fileOut)), keyValueSeparator);
			}
		}
	}*/
	
	public static String cacheFileLocation = "";
	public static boolean useCache = false;
	
	private static final Log LOG = LogFactory.getLog(Replace.class);
	
	// FIRST PHASE MAPPER1
	// two input files
	// 1. regular graph | assigns lid for every link structure and creates edges
	// 2. link - id mapping | output the same
	public static class GenerateEdges extends Mapper<Text, Text, Text, LinkAttrOrUrlMapping> {
		
		private HashMap<String, String> idmap;
		private static MessageDigest md5;
		
		@Override
		public void setup(Context context) {
			if (useCache)
				loadIdUrlMapping(context); //fills in the map (idmap)
			else
				idmap = new HashMap<String, String>(); //use empty map for cache value check
			
			md5 = Commons.getMD5();
		}
		
		byte[] digest;
		LinkAttrOrUrlMapping edge = new LinkAttrOrUrlMapping();
		Text txt = new Text();
		BytesWritable lid = new BytesWritable();
		
		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			
			//LOG.error("generateEdges-link" + key.toString() + "\t" + value.toString() );
			
			String[] destinations = value.toString().split(" ");
			
			// calculate lid source_url + timestamp
			digest = Commons.hash(md5, key.toString()+destinations[0]);
			lid.set(digest, 0, digest.length);
			
			edge.setEdgeUrl(lid , true);
			context.write(key, edge); 
			//LOG.error("generateEdges-edge: " + key.toString() + " " + edge.toString());
			
			//set the time
			edge.setEdgeTime(lid);
			txt.set(destinations[0]);
			context.write(txt, edge);
			//LOG.error("generateEdges-edge: " + txt.toString() + " " + edge.toString());
			
			for (int i = 1; i < destinations.length; i++) { // start from 1 so skip the time
				/*
				 * as the key, if mapping for that url is in the cache, then
				 * corresponding uid is written out, if not, url itself is
				 * written out  <(url | uid), (lid&issource)>
				 */
				edge.setEdgeUrl(lid, false);
				txt.set(getId(destinations[i]));
				context.write(txt, edge);
				//LOG.error("generateEdges-edge: " + txt.toString() + " " + edge.toString());
			}
		}
		
		private String getId(String url) {
			return (idmap.get(url) == null) ? url : idmap.get(url);
		}
		
		private void loadIdUrlMapping(Context context) {

			FSDataInputStream in = null;
			// DataInput
			BufferedReader br = null;
			try {
				FileSystem fs = FileSystem.get(context.getConfiguration());
				Path path = new Path(cacheFileLocation);
				in = fs.open(path);
				br = new BufferedReader(new InputStreamReader(in));
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
				System.out.println("read from distributed cache: file not found!");
			} catch (IOException e1) {
				e1.printStackTrace();
				System.out.println("read from distributed cache: IO exception!");
			}
			try {
				this.idmap = new HashMap<String, String>();
				String line = "";
				while ((line = br.readLine()) != null) {
					String[] arr = line.split("\t");
					if (arr.length == 2)
						idmap.put(arr[1], arr[0]);
				}
				in.close();

			} catch (IOException e1) {
				e1.printStackTrace();
				System.out.println("read from distributed cache: read length and instances");
			}
		}
	}
	
	// FIRST PHASE MAPPER2
	public static class GenerateUrls extends Mapper<LongWritable, Text, Text, LinkAttrOrUrlMapping> {

		LinkAttrOrUrlMapping mapping = new LinkAttrOrUrlMapping();
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			mapping.setMap(key.get());
			context.write(value, mapping);
			//LOG.error("generateURLs: " + value.toString() + " " + mapping.toString());
		}
	}
	
	/*
	// FIRST PHASE COMBINER
	public static class ReplacementCombiner extends Reducer<Text, LinkAttrOrUrlMapping, Text, LinkAttrOrUrlMapping> {
		
		RecordWriter<BytesWritable, LinkAttr> writer; 
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			writer = new CombineOutputFormat().getRecordWriter(context);
		}
		
		LinkAttr linkurl = new LinkAttr();
		BytesWritable bytesw = new BytesWritable();
		Text uid;
		boolean istime;
		ArrayList<LinkAttrOrUrlMapping> edges = new ArrayList<LinkAttrOrUrlMapping>();
		public void reduce(Text key, Iterable<LinkAttrOrUrlMapping> values, Context context) throws IOException, InterruptedException {
			edges.clear();
			uid = null;
			istime = false;
			for (LinkAttrOrUrlMapping val : values) {
				if (!val.isMap && val.is_time) // edge - time
				{
					linkurl.setTime(key);
					bytesw.set(val.getLid(), 0, val.getLid().length);
					writer.write(bytesw, linkurl);
					istime = true;
				}
				else
				{
					if (val.isMap)
					{
						uid = val.getUid();
						context.write(key, val); //always output url id - someothers might need it !
					}
					else // edge - source or dest
						edges.add(val);
				}
			}
			
			if (uid != null && !istime)
			{
				for (int i = 0; i < edges.size(); i++) {
					bytesw.set(edges.get(i).getLid(), 0, edges.get(i).getLid().length);
					linkurl.setUrl(uid, edges.get(i).is_source);
					writer.write(bytesw, linkurl);
				}
			}
			else if (uid == null)
			{
				for (int i = 0; i < edges.size(); i++) {
					context.write(key, edges.get(i));
				}
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			writer.close(context);
		}
	}
	*/
	
	// FIRST PHASE REDUCER
	public static class ReplacementReducer extends Reducer<Text, LinkAttrOrUrlMapping, BytesWritable, LinkAttr> {
		
		StringBuffer errormessage= new StringBuffer();
		
		BytesWritable lid = new BytesWritable();
		LinkAttr linkattr = new LinkAttr();
		LongWritable uid = new LongWritable();
		ArrayList<LinkAttrOrUrlMapping> edges = new ArrayList<LinkAttrOrUrlMapping>();
		boolean istime;
		boolean isuidset;
		LinkAttrOrUrlMapping newattr;
		
		ArrayList<LinkAttrOrUrlMapping> forme = new ArrayList<LinkAttrOrUrlMapping>();
		@Override
		public void reduce(Text key, Iterable<LinkAttrOrUrlMapping> values, Context context) throws IOException, InterruptedException {
			forme.clear();
			
			if (key.getLength() == 0)
				return;
			
			istime = false;
			isuidset = false;
			edges.clear();
			//LOG.error("inputs_in_reducer-key: " + key.toString());
			for (LinkAttrOrUrlMapping val : values) {
				
				//LOG.error("inputs_in_reducer-vals: "+val.toString());
				forme.add(val);
				
				if (val.isUrlIdMapping())
				{
					uid.set(val.getUid().get());
					isuidset = true;
				}
				else if (val.isTime())
				{
					lid.set(val.getLid());
					try{
						linkattr.setTime(Long.parseLong(key.toString()));
					}catch (NumberFormatException e){
						System.err.println("key:" + key.toString());
					}
					context.write(lid, linkattr);
					istime = true;
				}
				else 
				{
					if(isuidset){
						lid.set(val.getLid());
						linkattr.setUrl(uid, val.isSource());
						//LOG.error("replaced_attribute: " + lid.toString()+ "\t" + linkattr.toString());
						context.write(lid, linkattr);
					}
					else
					{
						newattr = new LinkAttrOrUrlMapping();
						newattr.setEdgeUrl(val.getLid(), val.isSource());
						edges.add(newattr);
					}
				}
			}
			
			if (!isuidset && key != null && key.getLength() > 0 && !istime) // no uid and not time, then it is a regular url without id - problem!
			{
				errormessage.setLength(0);
				for (LinkAttrOrUrlMapping val : forme)
					errormessage.append("val: "+val.toString() + "\n");
				LOG.error("url_without_id_exception key: "+ key.toString() +" values:\n" + errormessage.toString());
				throw new IllegalArgumentException("url withoud id! key: --" + key.toString() + "-- key.len: " + key.getLength() + " values.length: " + edges.size());
				//System.err.println("url withoud id! key: --" + key.toString() + "-- key.len: " + key.getLength() + " values.length: " + edges.size());
			}
			
			else if (!istime){
				for (int i = 0; i < edges.size(); i++) {
					lid.set(edges.get(i).getLid());
					linkattr.setUrl(uid, edges.get(i).isSource());
					//LOG.error("replaced_attribute: " + lid.toString()+ "\t" + linkattr.toString());
					context.write(lid, linkattr);
				}
			}
		}
	}
	
	// SECOND PHASE MAPPER
	public static class IdentityMapper extends Mapper<BytesWritable, LinkAttr, BytesWritable, LinkAttr> {
		
		public void setup(Context context) {
		}
		
		@Override
		public void map(BytesWritable key, LinkAttr value, Context context) throws IOException, InterruptedException {
			//LOG.info("PHASE2-MAP-in&out " + key.toString() + "\t" + value.toString());
			context.write(key, value);
		}
	}
	
	// SECOND PHASE REDUCER
	public static class BackToGraphReducer extends Reducer<BytesWritable, LinkAttr, Text, Text> {
		
		StringBuffer dests = new StringBuffer();
		Text source = new Text();
		Text destsTxt = new Text();
		@Override
		public void reduce(BytesWritable key, Iterable<LinkAttr> values, Context context) throws IOException, InterruptedException {
			//LOG.info("PHASE2-RED-in " + key.toString() );
			
			dests.delete(0, dests.length());
			for (LinkAttr val : values) {
				if (val.isSourceUrl())
					source.set(val.getUrlId().toString());
				else if (val.isTime())
					dests.insert(0, val.getUrlId().toString() + " ");
				else
					dests.append(val.getUrlId().toString() + " ");
			}
			
			destsTxt.set(dests.toString());
			//LOG.info("PHASE2-RED-out " + source.toString() + "\t" + destsTxt.toString());
			context.write(source, destsTxt);
		}
	}
  	
	static String linkgraph, urlmap, intermediate, replaced_linkgraph;
	
	//split out the edges and replace the urls
	private static void phase1(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.set("mapred.local.dir", "/mapred-local-cache");
		
		boolean isDefaultFilePath = false;
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length == 0) {
			System.err.println("INFO:Default file paths are in use");
			isDefaultFilePath = true;
			
			Replace.useCache = false;
			linkgraph = Commons.LINKGRAPH_PATH;
			urlmap = Commons.MAP_PATH;
			intermediate = Commons.LID_UID_INTERMIATE_PAIRS_PATH;
			replaced_linkgraph = Commons.REPLACED_WEBGRAPH_PATH;
		}
		else if (otherArgs.length != 5 && otherArgs.length != 6) {
			System.err.println("current args.length:" + otherArgs.length + " has to be 5 or 6");
			System.err.println("Usage: LinkGraphUrlIdReplacement <in graph path> <in map path> <intermediate output path> <final output path> <use cache (true/false)> <cache path (optional)>");
			System.exit(2);
		}
		else
		{
			linkgraph = otherArgs[0];
			urlmap = otherArgs[1];
			intermediate = otherArgs[2];
			replaced_linkgraph = otherArgs[3];
		}
		
		if (!isDefaultFilePath)
		{
			if (otherArgs[4].equalsIgnoreCase("true") || otherArgs[4].equalsIgnoreCase("T"))
				Replace.useCache = true;
			else
				Replace.useCache = false;
		}
		
		if (Replace.useCache)
		{
			if (otherArgs.length != 6){
				System.err.println("cache set to true, path has to be given !>");
				System.err.println("Usage: LinkGraphUrlIdReplacement <in graph path> <in map path> <intermediate output path> <final output path> <use cache (true/false)> <cache path (optional)>");
				System.exit(2);
			}
			else
				Replace.cacheFileLocation = otherArgs[5];
		}
		
		System.out.println("use cache:" +  useCache);
		
		Job job = Job.getInstance(new Cluster(conf), conf);
		job.setJobName("replace urls 'lid uid' format (everything sampled - 1000 partitions)");
		job.addCacheFile(new URI(cacheFileLocation));
		job.setJarByClass(Replace.class);
		
		//MultipleInputs.addInputPath(job, new Path(linkgraph), SequenceFileInputFormat.class, GenerateEdges.class);
		MultipleInputs.addInputPath(job, new Path(urlmap), SequenceFileInputFormat.class, GenerateUrls.class);
		MultipleInputs.addInputPath(job, new Path(linkgraph), SequenceFileInputFormat.class, GenerateEdges.class);
		//MultipleInputs.addInputPath(job, new Path(urlmap), IdUrlMapInputFormat.class, GenerateUrls.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LinkAttrOrUrlMapping.class);
		
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		//job.setPartitionerClass(SampleBasedPartitioner.class);
		
		job.setReducerClass(ReplacementReducer.class);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setOutputKeyClass(BytesWritable.class);
		job.setOutputValueClass(LinkAttr.class);
		
		job.setNumReduceTasks(30);
		FileOutputFormat.setOutputPath(job, new Path(intermediate));
		job.waitForCompletion(true);
	}
	
	//build the graph back
	private static void phase2(String[] args) throws Exception  {
		Configuration conf2 = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf2, args).getRemainingArgs();
		conf2.set("mapred.local.dir", "/mapred-local-cache");
		
		Job job2 = Job.getInstance(new Cluster(conf2), conf2);
		job2.setJobName("build the graph back from 'lid uid' pairs");
		
		job2.setJarByClass(Replace.class);
		
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job2, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job2, GzipCodec.class);
		
		job2.setMapOutputKeyClass(BytesWritable.class);
		job2.setMapOutputValueClass(LinkAttr.class);
		
		job2.setMapperClass(IdentityMapper.class);
		job2.setReducerClass(BackToGraphReducer.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setNumReduceTasks(20);
		FileInputFormat.addInputPath(job2, new Path(intermediate));
		FileOutputFormat.setOutputPath(job2, new Path(replaced_linkgraph));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		phase1(args);
		phase2(args);
	}
}
