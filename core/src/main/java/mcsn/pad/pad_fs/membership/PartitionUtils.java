package mcsn.pad.pad_fs.membership;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Assert;

import it.cnr.isti.hpclab.consistent.ConsistentHasher;
import it.cnr.isti.hpclab.consistent.ConsistentHasherImpl;

public class PartitionUtils {
	
	public static ConsistentHasher<String, String> getConsistentHasher() {
		return new ConsistentHasherImpl<>(
				ConsistentHasherImpl.VIRTUAL_INSTANCES_PER_BUCKET, 
				ConsistentHasher.getStringToBytesConverter(), 
				ConsistentHasher.getStringToBytesConverter(),
				ConsistentHasher.SHA1);
	}
	
	public static String getCoordinator(List<String> servers, String key) {
		
		ConsistentHasher<String, String> cHasher = getConsistentHasher();
		
		for (String s : servers) {
			cHasher.addBucket(s);
		}
		
		cHasher.addMember(key);
		Map<String, List<String>> map = cHasher
				.getAllBucketsToMembersMapping();
		
		String coordinator = null;
		for (Map.Entry<String, List<String>> e : map.entrySet()) {
			if (e.getValue().size() != 0) {
				coordinator = e.getKey();
				break;
			}
		}
		
		Assert.assertNotNull(coordinator);
		return coordinator;
	}
	
	public static List<String> getPreferenceList(List<String> servers, String key, int N) throws InterruptedException {
		
		ConsistentHasher<String, String> cHasher = getConsistentHasher();
		
		for (String s : servers) {
			cHasher.addBucket(s);
		}
		cHasher.addMember(key);
		
		Map<String, List<String>> map;
		List<String> sortedBuckets = new ArrayList<String>();
		while (cHasher.getAllBuckets().size() != 0 && sortedBuckets.size() != N) {
			map = cHasher.getAllBucketsToMembersMapping();
			for (Map.Entry<String, List<String>> e : map.entrySet()) {
				if (e.getValue().size() != 0 || cHasher.getAllBuckets().size() == 1) {
					sortedBuckets.add(e.getKey());
					cHasher.removeBucket(e.getKey());
					break;
				}
			}
		}
		
		return sortedBuckets;
	}
	
	/** 
	 * Print bucket distribution for the cases in which the number of
	 * virtual nodes is equal to 1, to 100, or to 700. 
	 */
	public static void printBucketDistribution(ConsistentHasher<String, String> cHasher) {
				
		Map<Integer, Map<Double, String>> result = ConsistentHasherImpl
				.getDistributionPercentage(1, ConsistentHasherImpl.VIRTUAL_INSTANCES_PER_BUCKET, 
						ConsistentHasher.getStringToBytesConverter(),
						ConsistentHasher.getStringToBytesConverter(), 
						ConsistentHasher.getSHA1HashFunction(),
						cHasher.getAllBuckets(), 
						cHasher.getAllMembers());

		
		result.forEach( (virtNodeId, map) -> {
			if (virtNodeId == 1 || virtNodeId == 100 || virtNodeId == 700) {
				System.out.println("No of virt nodes : " + virtNodeId);
				System.out.printf("%5s%10s\n", "%", "BucketId");
				map.forEach((percent, node) -> {
					System.out.printf("%5.2f %s\n", percent, node);
				});
				System.out.println("\n\n");
			}
		});
		
	}
}
