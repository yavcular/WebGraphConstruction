package nyu.cs.webgraph.structures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

// FIRST PHASE OUTPUT VALUE
public class LinkAttr implements WritableComparable<LinkAttr> {
	private boolean isTime;
	private boolean isSource;
	private LongWritable uid = new LongWritable();

	public LinkAttr() {
	}
	
	public void setUrl(LongWritable uid, boolean is_s) {
		this.isTime = false;
		this.isSource = is_s;
		this.uid.set(uid.get());
	}
/*
	public void setUrl(long uid, boolean is_s) {
		this.isTime = false;
		this.isSource = is_s;
		this.uid.set(uid);
	}*/
	
	public void setTime(long time) {
		this.isTime = true;
		this.isSource = false;
		this.uid.set(time);
	}
	
	public boolean isTime(){
		return isTime;
	}
	public boolean isSourceUrl(){
		return isSource;
	}
	public LongWritable getUrlId(){
		return uid;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		isTime = in.readBoolean();
		isSource = in.readBoolean();
		uid.set(in.readLong());
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isTime);
		out.writeBoolean(isSource);
		out.writeLong(uid.get());
	}

	@Override
	public int compareTo(LinkAttr e) {
		if (this.isSource == e.isSource && this.isTime == e.isTime && e.uid.toString().equalsIgnoreCase(this.uid.toString()))
			return 0;
		return 1;
	}
	
	@Override
	public String toString() {
		return uid.get() + " time?" +  isTime + " source?" + isSource;
		
	}
}
