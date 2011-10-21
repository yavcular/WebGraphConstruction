package nyu.cs.webgraph.structures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

//FIRST PHASE INTERMEDIATE PAIR VALUE
public class KeyHashOrNoRepHolder implements WritableComparable<KeyHashOrNoRepHolder> {

	private BytesWritable keyHash;
	private Text noRep; 
	
	public KeyHashOrNoRepHolder() {
		keyHash = new BytesWritable();
		noRep = new Text();
	}
	
	public void set(byte[] slid) {
		this.keyHash.set(slid, 0, slid.length);
	}
	
	public void set(String value) {
		this.noRep.set(value);
	}
	
	public Text getNoRep(){
		return noRep;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		keyHash= new BytesWritable();
		keyHash.readFields(in);
		noRep = new Text();
		noRep.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		keyHash.write(out);
		noRep.write(out);
	}
	
	@Override
	public int compareTo(KeyHashOrNoRepHolder e) {
		if (!keyHash.equals(this.keyHash) || !noRep.equals(this.noRep) )
			return 1;
		return 0;
	}
	
}