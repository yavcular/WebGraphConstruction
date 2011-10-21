package nyu.cs.webgraph.structures;

import java.util.HashSet;

public class SingleCrawl extends SingleCrawlOutlinkList implements Comparable<SingleCrawl>{
	Long timestamp;
	
	public SingleCrawl() {
		super();
	}
	public void setTimeStamp(Long timeStamp){
		this.timestamp = timeStamp;
	}
	public Long getTimestamp(){
		return timestamp;
	}
	
	@Override
	public int compareTo(SingleCrawl sc) {
		return this.timestamp.compareTo(sc.timestamp);
	}
}
