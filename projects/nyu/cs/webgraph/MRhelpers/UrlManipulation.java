package nyu.cs.webgraph.MRhelpers;

public class UrlManipulation {
	
	/*
	 * if has ://
	 * drop everything till this pattern, get all btw :// and first /
	 * 
	 * else if has non of these
	 * don't drop anything at the beginning, gel all till first /
	 * 
	 */
	public static String getBaseDomain(StringBuffer url){
		
		int start, end; 
		start = url.indexOf("://");
		if (start > 5)
			start = -1; //not at the beginning
		
		if ( start == -1) //does not have ://
		{
			start = url.indexOf(":");
			if (start > 5)
				start = -1; //not at the beginning
			
			if (start == -1)
				start = 0;
			else
				start += 1;
		}
		else // have ://
			start +=3; 
		
		if (url.indexOf("www.", start) == start)
			start += 4;
		
		int endSearchStartPos = start; 
		while(endSearchStartPos < url.length() && url.charAt(endSearchStartPos) == '/')
			endSearchStartPos++;
		end = url.indexOf("/", endSearchStartPos);
		start = endSearchStartPos; // skips the slashes at the beginning
		
		if (end == -1)
		{
			end = url.length();
		}
		
		return url.substring(start,end);
	}
	
	public static String getBaseDomain(String url){
		return getBaseDomain(new StringBuffer(url));
	}
	
	public static String normalize(String url){
		if (url.startsWith("http://"))
			return url.substring(7);
		else
			return url;
	}
}