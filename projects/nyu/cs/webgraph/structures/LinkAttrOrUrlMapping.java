package nyu.cs.webgraph.structures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

//FIRST PHASE INTERMEDIATE PAIR VALUE
public class LinkAttrOrUrlMapping implements WritableComparable<LinkAttrOrUrlMapping> {
	private boolean isMap; //map or not
	
	//if map has uid only
	private LongWritable uid = new LongWritable(); 
	
	//if edge has lid and isSource 
	private boolean is_time; //then source or dest
	private boolean is_source;
	private BytesWritable lid = new BytesWritable();
	
	public LinkAttrOrUrlMapping() {
	}
	public void setEdgeTime(BytesWritable lid) {
		setEdge(lid);
		this.is_time= true;
		this.is_source = false;
	}
	
	public void setEdgeUrl(BytesWritable lid, boolean is_source) {
		setEdge(lid);
		this.is_time = false;
		this.is_source = is_source;
	}
	
	/*private void setEdge(byte[] lid){
		this.isMap = false;
		this.uid = null;
		
		this.lid.set(lid, 0, lid.length);
	}*/
	private void setEdge(BytesWritable lid){
		this.isMap = false;
		this.uid = null;
		
		this.lid.set(lid);
	}
	
	public void setMap(long uid) {
		this.isMap = true;
		
		this.uid.set(uid);
		
		this.lid = null;
		this.is_time = false;
		this.is_source = false;
	}
	
	public boolean isSource(){
		return is_source;
	}
	
	public boolean isTime(){
		return is_time;
	}
	public boolean isUrlIdMapping(){
		return isMap;
	}
	
	public LongWritable getUid() {
		return this.uid;
	}
	public BytesWritable getLid(){
		return lid;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		isMap = in.readBoolean();
		if (isMap){
			uid = new LongWritable(in.readLong());
			//uid.set(in.readLong());
			lid = null;
		}
		else
		{
			is_time = in.readBoolean();
			is_source = in.readBoolean();
			lid = new BytesWritable();
			lid.readFields(in);
			//lid.readFields(in);
			uid = null;
		}
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isMap);
		if (isMap){
			out.writeLong(uid.get());
		}
		else
		{
			out.writeBoolean(is_time);
			out.writeBoolean(is_source);
			lid.write(out);
		}
	}
	
	@Override
	public int compareTo(LinkAttrOrUrlMapping e) {
		if (!e.lid.equals(this.lid) || this.is_source != e.is_source || this.is_time != e.is_time || this.isMap != e.isMap)
			return 1;
		return 0;
	}
	
	public String toString(){
		if (uid == null && lid != null)
			return "mapping?" + isMap + " uid=null" + " lid:" + lid.toString() + " time?" + is_time + " source?" + is_source;
		else if (uid != null && lid == null)
			return "mapping?" + isMap + " uid" + uid.toString() +" lid=null" + " time?" + is_time + " source?" + is_source;
		else if (uid == null && lid == null)
			return "mapping?" + isMap + " somethingiswrong_bothnull";
		else {
			return "mapping?" + isMap + " somethingiswrong_bothnotnull" + " uid:" + uid.toString() + " lid:" + lid.toString() + " time?" + is_time + " source?" + is_source;
		}
	}
}