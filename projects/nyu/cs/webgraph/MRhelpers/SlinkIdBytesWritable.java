package nyu.cs.webgraph.MRhelpers;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SlinkIdBytesWritable extends BinaryComparable
implements WritableComparable<BinaryComparable> {

	  private static final Log LOG = LogFactory.getLog(BytesWritable.class);
	  private static final int LENGTH_BYTES = 4;
	  private static final byte[] EMPTY_BYTES = {};

	  private int size;
	  private byte[] bytes;
	  
	  /**
	   * Create a zero-size sequence.
	   */
	  public SlinkIdBytesWritable() {this(EMPTY_BYTES);}
	  
	  /**
	   * Create a BytesWritable using the byte array as the initial value.
	   * @param bytes This array becomes the backing storage for the object.
	   */
	  public SlinkIdBytesWritable(byte[] bytes) {
	    this.bytes = bytes;
	    this.size = bytes.length;
	  }
	  
	  /**
	   * Get the data from the BytesWritable.
	   * @return The data is only valid between 0 and getLength() - 1.
	   */
	  public byte[] getBytes() {
	    return bytes;
	  }

	  /**
	   * Get the data from the BytesWritable.
	   * @deprecated Use {@link #getBytes()} instead.
	   */
	  @Deprecated
	  public byte[] get() {
	    return getBytes();
	  }

	  /**
	   * Get the current size of the buffer.
	   */
	  public int getLength() {
	    return size;
	  }

	  /**
	   * Get the current size of the buffer.
	   * @deprecated Use {@link #getLength()} instead.
	   */
	  @Deprecated
	  public int getSize() {
	    return getLength();
	  }
	  
	  /**
	   * Change the size of the buffer. The values in the old range are preserved
	   * and any new values are undefined. The capacity is changed if it is 
	   * necessary.
	   * @param size The new number of bytes
	   */
	  public void setSize(int size) {
	    if (size > getCapacity()) {
	      setCapacity(size);
	    }
	    this.size = size;
	  }
	  
	  /**
	   * Get the capacity, which is the maximum size that could handled without
	   * resizing the backing storage.
	   * @return The number of bytes
	   */
	  public int getCapacity() {
	    return bytes.length;
	  }
	  
	  /**
	   * Change the capacity of the backing storage.
	   * The data is preserved.
	   * @param new_cap The new capacity in bytes.
	   */
	  public void setCapacity(int new_cap) {
	    if (new_cap != getCapacity()) {
	      byte[] new_data = new byte[new_cap];
	      if (new_cap < size) {
	        size = new_cap;
	      }
	      if (size != 0) {
	        System.arraycopy(bytes, 0, new_data, 0, size);
	      }
	      bytes = new_data;
	    }
	  }

	
	  /**
	   * Set the value to a copy of the given byte range
	   * @param newData the new values to copy in
	   * @param offset the offset in current bytes to start at
	   * @param length the number of bytes to copy
	   */
	  public void copy(byte[] newData, int offset, int length) {
	    System.arraycopy(newData, 0, bytes, offset, length);
	  }

	  /**
	   * Set the BytesWritable to the contents of the given newData1 and newData2 in given order.
	   * @param newData the value to set this BytesWritable to.
	   */
	  public void set(byte[] newData1, byte[] newData2) {
		  setSize(0);
		  setSize(newData1.length + newData2.length);
		  copy(newData1, 0, newData1.length);
		  copy(newData2, newData1.length, newData2.length);
	  }
	  
	  /**
	   * Set the BytesWritable to the contents of the given newData1 and newData2 in given order.
	   * @param newData the value to set this BytesWritable to.
	   */
	  public void set(byte[] newData) {
		  setSize(0);
		  setSize(newData.length);
		  copy(newData, 0, newData.length);
	  }
	  
	  // inherit javadoc
	  public void readFields(DataInput in) throws IOException {
	    setSize(0); // clear the old data
	    setSize(in.readInt());
	    in.readFully(bytes, 0, size);
	  }
	  
	  public int hashCode() {
	    return super.hashCode();
	  }

	  /**
	   * Are the two byte sequences equal?
	   */
	  public boolean equals(Object right_obj) {
	    if (right_obj instanceof BytesWritable)
	      return super.equals(right_obj);
	    return false;
	  }

	  /**
	   * Generate the stream of bytes as hex pairs separated by ' '.
	   */
	  public String toString() { 
	    StringBuilder sb = new StringBuilder(3*size);
	    for (int idx = 0; idx < size; idx++) {
	      // if not the first, put a blank separator in
	      if (idx != 0) {
	        sb.append(' ');
	      }
	      String num = Integer.toHexString(0xff & bytes[idx]);
	      // if it is only one digit, add a leading 0.
	      if (num.length() < 2) {
	        sb.append('0');
	      }
	      sb.append(num);
	    }
	    return sb.toString();
	  }

	  /** A Comparator optimized for BytesWritable. */ 
	  public static class Comparator extends WritableComparator {
	    public Comparator() {
	      super(BytesWritable.class);
	    }
	    
	    /**
	     * Compare the buffers in serialized form.
	     */
	    public int compare(byte[] b1, int s1, int l1,
	                       byte[] b2, int s2, int l2) {
	      return compareBytes(b1, s1+LENGTH_BYTES, l1-LENGTH_BYTES, 
	                          b2, s2+LENGTH_BYTES, l2-LENGTH_BYTES);
	    }
	  }
	  
	  static {                                        // register this comparator
	    WritableComparator.define(BytesWritable.class, new Comparator());
	  }
	  public void write(DataOutput out) throws IOException {
		      out.writeInt(size);
		      out.write(bytes, 0, size);
		    }

	  public byte[] getBytesAfter(int startPos){
		  if (startPos > getLength())
			  throw new IllegalArgumentException("start pos " + startPos + "is larger than current size" + getLength());
		  byte[] retbytes = new byte[getLength()-1];
		  System.arraycopy(getBytes(), startPos, retbytes, 0, getLength()-1);
		  return retbytes;
	  }
	
}
