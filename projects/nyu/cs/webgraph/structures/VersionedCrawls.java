package nyu.cs.webgraph.structures;

import java.util.HashMap;
import java.util.HashSet;

public class VersionedCrawls{
	
	
	private String[] outlinks;
	private String[] timestamps;
	private String[] bitvector;
	
	HashMap<String, HashSet<String>> timeToLinkMapping; 
	
	public VersionedCrawls() {
		
	}
	
	public void addOutlink(String[] linkList){
		this.outlinks = linkList;
	}
	public void addTimestamps(String[] timestamp){
		this.timestamps = timestamp;
	}
	public void addBitvectors(String[] bitvector){
		this.bitvector = bitvector;
	}
	
	public void construct(){
		if (timestamps == null)
			return; 
		
		char[] currBV; 
		timeToLinkMapping = new HashMap<String, HashSet<String>>();
		for (int tsPos = 0; tsPos < timestamps.length; tsPos++) {
			if (!timeToLinkMapping.containsKey(timestamps[tsPos])){
				timeToLinkMapping.put(timestamps[tsPos], new HashSet<String>());
			}
			
			if(outlinks != null && bitvector != null && bitvector.length > 0 && outlinks.length > 0)
			{
				currBV = bitvector[tsPos].toCharArray();
				if (currBV.length < outlinks.length)
					throw new IllegalArgumentException("bitvector length" + currBV.length + " and oulinks legth" + outlinks.length + "  does not match!");
				
				for (int bitPos = 0; bitPos < outlinks.length; bitPos++) {
					
					if (currBV[bitPos] == '1')
						timeToLinkMapping.get(timestamps[tsPos]).add(outlinks[bitPos]);
				}
			}
		}
	}
	
	public void clear(){
		outlinks = null; 
		timestamps = null; 
		bitvector = null;
	}
	
	public boolean hasLink(String timestamp, String link)
	{
		if(timeToLinkMapping!=null){
			if (timeToLinkMapping.get(timestamp) != null)
				return timeToLinkMapping.get(timestamp).contains(link);
			else 
				return false;
		}
		else return false;
	}
	
}
