package nyu.cs.webgraph.MRhelpers;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.twmacinta.util.MD5;

public class Commons {
	
	public static final String BASE_PATH = "/user/yasemin/wg/";
	
	public static final String METADATA_PATH = BASE_PATH+"input/NLI-IE-HISTORICAL-IE-DOMAIN/";
	public static final String LINKGRAPH_PATH = BASE_PATH+"output/linkgraph";
	public static final String UNIQUEURLS_PATH= BASE_PATH+"output/urlmap";
	public static final String MAP_PATH= BASE_PATH+"output/urlmap/urlmap-withids";
	public static final String LID_UID_INTERMIATE_PAIRS_PATH= BASE_PATH+"output/urlmap/intermediate";
	public static final String REPLACED_WEBGRAPH_PATH = BASE_PATH+"output/urlmap/final";
	public static final String FINAL_LAYERS_PATH = BASE_PATH+"output/urlmap/final-layers";
	
	public static final String ssampleCounter_PATH = "/user/yasemin/delis/ssample-counter";
	
	
	//for sampling
	
	private static final int expectedNumOfPagesCrawled_FOCUSED = 14756301; // user provided
	private static final long expectedNumOfTotalUrls_FOCUSED = 405216882; // user provided
	
	private static final int expectedNumOfPagesCrawled_IECRAWL = 102000000; // user provided
	private static final long expectedNumOfTotalUrls_IECRAWL = 3883555060L; // user provided
	
	private static final int samplePartitions = 1000; //user provided
	private static final int expectedNumOfPagesCrawled= expectedNumOfPagesCrawled_IECRAWL * 5 / 3; // user provided
	private static final long expectedNumOfTotalUrls = expectedNumOfTotalUrls_IECRAWL * 5 / 3; // user provided
	
	public static final int fudgeFactor = 1000;
	public static final double sampleFrequency = (double)(samplePartitions * fudgeFactor) / expectedNumOfTotalUrls;
	
	public static final String sampleDir = BASE_PATH+"output/sample/";
	public static final String sampleMergedDir = BASE_PATH+"output/sample-merged";
	public static final String sampleBoundriesDir = BASE_PATH+"output/sample-partition-boundries";
	
	//hashing with standar lib
	public static MessageDigest getMD5(){
		try {
			return MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}
	public static byte[] hash(MessageDigest md, String s){
		md.update(s.getBytes()); //get a hash val from url+time
		return md.digest();
	}
	
	//fast hashing
	public static MD5 getFastMD5(){
		return new MD5(); 
	}
	public static byte[] hash(MD5 md, String s){
		md.Update(s);
		return md.Final();
	}
	public static String hashToStr(MD5 md, String s){
		md.Update(s);
		return md.asHex();
	}

}
