package nyu.cs.webgraph.analysis;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MortalityCount {

	
	public static void main(String[] args) throws IOException {
		
		String inFile = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		DataInputStream d = new DataInputStream(fs.open(new Path(inFile)));
		BufferedReader reader = new BufferedReader(new InputStreamReader(d));
		
		int total=0, added=0, removed=0;
		String[] curr;
		String curr_line;
		
		int onlyadded=0, onlyremoved=0, both=0;
		
		while((curr_line = reader.readLine())!=null){
			curr = curr_line.split("\t")[1].split(" ");
			total += Integer.parseInt(curr[0]);
			added += Integer.parseInt(curr[1]);
			removed += Integer.parseInt(curr[2]);
			
			if(Integer.parseInt(curr[1]) != 0 && Integer.parseInt(curr[2]) != 0)
				both++;
			else if (Integer.parseInt(curr[1]) != 0 )
				onlyadded++;
			else if (Integer.parseInt(curr[2]) != 0 )
				onlyremoved++;
				
		}
		reader.close();
		System.out.println(total + " " +onlyadded +" " + onlyremoved+" " + both);
		
	}
	
	
}
