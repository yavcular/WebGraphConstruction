package nyu.cs.webgraph.structures;

import java.util.ArrayList;
import java.util.HashSet;

public class SingleCrawlOutlinkList{
	protected HashSet<String> outlinks; 
	
	public SingleCrawlOutlinkList() {
		outlinks = new HashSet<String>();
	}
	
	public void addOutlink(String link){
		outlinks.add(link);
	}
	public void addOutlink(ArrayList<String> linkList){
		outlinks.addAll(linkList);
	}
	
	public boolean hasLink(String link)
	{
		return outlinks.contains(link);
	}
	
}
