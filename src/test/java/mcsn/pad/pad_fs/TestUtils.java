package mcsn.pad.pad_fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.json.JSONException;

import it.cnr.isti.hpclab.consistent.ConsistentHasher;
import it.cnr.isti.hpclab.consistent.ConsistentHasherImpl;
import voldemort.versioning.Versioned;

public class TestUtils {

	public static String nextSessionId(Random random) {
	    return new BigInteger(100, random).toString(32);
	}
	
	public static void printBucketDistribution() {
		Random random = new Random(System.currentTimeMillis());
		List<Integer> buckets = new ArrayList<>();
		for (int i = 0; i < 10; i++)
			buckets.add(random.nextInt(Integer.MAX_VALUE) / 5);
		List<Integer> members = new ArrayList<>();
		for (int i = 0; i < 10000; i++)
			members.add(random.nextInt());
		Map<Integer, Map<Double, Integer>> result = ConsistentHasherImpl
				.getDistributionPercentage(1, 800,
						ConsistentHasher.getIntegerToBytesConverter(),
						ConsistentHasher.getIntegerToBytesConverter(),
						ConsistentHasher.getSHA1HashFunction(), buckets,
						members);
		result.forEach((virtNodeId, map) -> {
			System.out.println("No of virt nodes : " + virtNodeId);
			System.out.printf("%5s%10s\n", "%", "BucketId");
			map.forEach((percent, barray) -> {
				System.out.printf("%5.2f %d\n", percent, barray);
			});
			System.out.println("\n\n");
		});
	}
	
	public static void printLoadDistribution(ConsistentHasher<String, String> cHasher) {
		Map<String, List<String>> map = cHasher.getAllBucketsToMembersMapping();
		for (Map.Entry<String, List<String>> entry : map.entrySet()) {
			System.out.println(entry.getKey() + " : ");
			System.out.println("\t"+entry.getValue().size());
		}
	}
	
	public static List<Node> getMembers(int dim) throws FileNotFoundException, JSONException, IOException, InterruptedException {
		
		String filename = TestUtils.class.getResource("/gossip.conf").getFile();
		File configFile = new File(filename);
		
		List<Node> members = new ArrayList<>();
		for (int i = 1; i <= dim; i++) {
			members.add(i-1,  new Node("127.0.0."+i, configFile));
		}
		
		return members;
	}
	
	public static List<Versioned<byte[]>> getElements(int dim) {

		Random random = new Random(System.currentTimeMillis());
		ArrayList<Versioned<byte[]>> elements = new ArrayList<>();
		
		for (int i=0; i<dim; i++) {
			elements.add( new Versioned<byte[]>( nextSessionId(random).getBytes()) );
		}
		
		return elements;
	}
}
