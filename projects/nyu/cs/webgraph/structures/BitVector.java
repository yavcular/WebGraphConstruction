package nyu.cs.webgraph.structures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;

import org.apache.hadoop.io.WritableComparable;

public class BitVector extends BitSet implements WritableComparable<BitVector> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private int vectorSize;
	
	public BitVector(int size)
	{
		super();
		vectorSize = size;
	}
	
	@Override
	/*
	 * Size() method of BitSet is not accurate
	 */
	public int size(){
		return vectorSize;
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer("");
		for (int i = 0; i < size(); i++) {
			sb.append(get(i) ? "1" : "0");
		}
		return sb.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		for (int i = 0; i < size; i++) 
			set(i, in.readInt() == 1 ? true : false);
		
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(size());
		for (int i = 0; i < size(); i++)
			out.writeInt(get(i) ? 1 : 0);
		
	}
	
	@Override
	public int compareTo(BitVector arg0) {
		return equals(arg0) ? 0 : 1;
	}
}
