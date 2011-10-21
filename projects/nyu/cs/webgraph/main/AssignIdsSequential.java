package nyu.cs.webgraph.main;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Date;


import nyu.cs.webgraph.MRhelpers.Commons;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.GenericOptionsParser;

public class AssignIdsSequential {

	FileSystem fs;
	Path inPath, outPath;

	public AssignIdsSequential(Configuration conf, Path inPath, Path outPath) throws IOException {
		this.fs = FileSystem.get(conf);
		this.inPath = inPath;
		this.outPath = outPath;
	}

	public void assign() throws IOException {

		if (fs.exists(outPath)) {
			throw new IllegalArgumentException("one of output paths already exists ! ");
		} else if (!fs.exists(inPath)) {
			throw new IllegalArgumentException("input path does not exists ! ");
		}
		
		if (fs.exists(outPath))
			throw new IllegalArgumentException("outpath already exists");
		else
			fs.mkdirs(outPath);

		if (!fs.isDirectory(inPath) || !fs.isDirectory(outPath))
			throw new IllegalArgumentException("inPath or outPath is not a directory!");
		
		Path currOutFile;
		long id = 0;
		
		
		//read regular file write regular file
		/*BufferedWriter bwout = null;
		BufferedReader brinput = null;
		for (FileStatus currInFile : fs.listStatus(inPath)) {
			currOutFile = new Path(outPath, currInFile.getPath().getName());
			
			bwout = new BufferedWriter(new OutputStreamWriter(fs.create(currOutFile)));
			brinput = new BufferedReader(new InputStreamReader(fs.open(currInFile.getPath())));
			String line;
			// not declared within while loop
			while ((line = brinput.readLine()) != null) {
				bwout.append(id + "\t" + line);
				bwout.append(System.getProperty("line.separator"));
				id++;
			}
			brinput.close();
			
			bwout.flush();
			bwout.close();
			
			fs.delete(currInFile.getPath(), false);
		}*/
		
		//read seq file write seq file
		SequenceFile.Writer writer;
		SequenceFile.Reader reader;
		//CompressionCodec codec = new GzipCodec();
		NullWritable nw = NullWritable.get();
		Text url = new Text();
		LongWritable lw = new LongWritable();
		for (FileStatus currInFile : fs.listStatus(inPath)) {
			currOutFile = new Path(outPath, currInFile.getPath().getName());
			
			reader = new SequenceFile.Reader(fs, currInFile.getPath(), new Configuration());
			writer = SequenceFile.createWriter(fs, new Configuration(), currOutFile, LongWritable.class, Text.class);//, CompressionType.RECORD, codec);
			while (reader.next(url, nw)) {
				lw.set(id);
				writer.append(lw, url);
				id++;
			}
			reader.close();
			writer.close();
			//fs.delete(currInFile.getPath(), false);
		}
		//read sequence file write regular file
		//prefer to use same format for reading & writing, otherwise, fails some cases!
		/*BufferedWriter writer;
		SequenceFile.Reader reader;
		NullWritable nw = NullWritable.get();
		Text url = new Text();
		for (FileStatus currInFile : fs.listStatus(inPath)) {
			currOutFile = new Path(outPath, currInFile.getPath().getName());
			
			reader = new SequenceFile.Reader(fs, currInFile.getPath(), new Configuration());
			writer = new BufferedWriter(new OutputStreamWriter(fs.create(currOutFile)));
			while (reader.next(url, nw)) {
				writer.append(id + "\t" + url.toString());
				writer.append(System.getProperty("line.separator"));
				id++;
			}
			writer.close();
			reader.close();
			//fs.delete(currInFile.getPath(), false);
		}*/
	}
	
	/**
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		long start = new Date().getTime();
		boolean isDefaultFilePath = false;

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Default file paths are in use\nUsage: RawGraphToLinkGraph <in> <out>");
			isDefaultFilePath = true;
		} else {

			AssignIdsSequential ai;
			if (isDefaultFilePath)
				ai = new AssignIdsSequential(conf, new Path(Commons.UNIQUEURLS_PATH), new Path(Commons.MAP_PATH));
			else
				ai = new AssignIdsSequential(conf, new Path(otherArgs[0]), new Path(otherArgs[1]));
			ai.assign();
		}
		System.out.println("assigning ids completed in " + (new Date().getTime() - start) / 1000 + "secs");
	}
}
