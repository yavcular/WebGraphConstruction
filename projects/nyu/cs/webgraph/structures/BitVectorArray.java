package nyu.cs.webgraph.structures;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;

import org.apache.hadoop.io.WritableComparable;

public class BitVectorArray implements WritableComparable<BitVectorArray> {

	
	BitVector[] bvArr; 
	int length; 
	int vectorSize;
	
	public BitVectorArray(int arrLength, int sizeOfEachVector){
		this.length = arrLength;
		vectorSize = sizeOfEachVector;
		initialize();
	}
	private void initialize(){
		bvArr = new BitVector[length];
		for (int i = 0; i < bvArr.length; i++) {
			bvArr[i] = new BitVector(vectorSize);
		}
	}
	
	public int getArrLength(){
		return bvArr.length;
	}
	
	public int getVectorSize(){
		return bvArr[0].size();
	}
	
	public void set(int vectorIndex, int bitIndex)
	{
		bvArr[vectorIndex].set(bitIndex);
	}
	public boolean get(int vectorIndex, int bitIndex)
	{
		return bvArr[vectorIndex].get(bitIndex);
	}
	
	@Override
	public String toString(){
		/*StringBuffer sb = new StringBuffer("");
		for (int i = 0; i < getArrLength(); i++) {
			sb.append(bvArr[i].toString());
		}
		return sb.toString();*/
		return "salak!";
	}
	
	public String toString(int vectorIntex){
		return bvArr[vectorIntex].toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		length = in.readInt();
		vectorSize = in.readInt();
		initialize();
		for (int i = 0; i < length; i++)
			for (int j = 0; j < length; j++) {
				bvArr[i].set(j, in.readInt() == 1 ? true : false);
			}
		
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(getArrLength());
		out.writeInt(getVectorSize());
		for (int i = 0; i < getArrLength(); i++){
			for (int j = 0; j < getVectorSize(); j++) {
				out.writeInt(get(i, j) ? 1 : 0);
			}
		}
	}
	
	@Override
	public int compareTo(BitVectorArray arg0) {
		return equals(arg0) ? 0 : 1;
	}
}
