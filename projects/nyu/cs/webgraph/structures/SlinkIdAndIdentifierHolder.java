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
public class SlinkIdAndIdentifierHolder implements WritableComparable<SlinkIdAndIdentifierHolder> {

	private BytesWritable slid;
	private ByteWritable identifier;
	private Text value; 
	
	public SlinkIdAndIdentifierHolder() {
		slid = new BytesWritable();
		identifier = new ByteWritable();
		value = new Text();
	}
	
	public boolean is(byte validentifier) {
		return validentifier == identifier.get();
	}
	
	public void set(byte[] slid, byte validentifier) {
		this.slid.set(slid, 0, slid.length);
		this.identifier.set(validentifier);
	}
	
	public void set(String value, byte validentifier) {
		this.value.set(value);
		this.identifier.set(validentifier);
	}
	
	public BytesWritable getSlid(){
		return slid;
	}
	public Text getValue(){
		return value;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		identifier = new ByteWritable();
		identifier.readFields(in);
		slid = new BytesWritable();
		slid.readFields(in);
		value = new Text();
		value.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		identifier.write(out);
		slid.write(out);
		value.write(out);
	}
	
	@Override
	public int compareTo(SlinkIdAndIdentifierHolder e) {
		if (!identifier.equals(this.identifier) || !slid.equals(this.slid) )
			return 1;
		return 0;
	}
	
}