package mcsn.pad.pad_fs.partitioning;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import it.cnr.isti.hpclab.consistent.ConsistentHasher;
import it.cnr.isti.hpclab.consistent.ConsistentHasherImpl;

public class PartitionUtils {
	
	public static ConsistentHasher<String, String> getConsistentHasher() {
		return new ConsistentHasherImpl<>(
				1, 
				ConsistentHasher.getStringToBytesConverter(), 
				ConsistentHasher.getStringToBytesConverter(),
				ConsistentHasher.SHA1);
	}
	
	public static String getCoordinator(List<String> servers, String key) {
		
		String coordinator = null;
		ConsistentHasher<String, String> cHasher = getConsistentHasher();
		
		for (String s : servers) {
			cHasher.addBucket(s);
		}
		
		cHasher.addMember(key);
		Map<String, List<String>> map = cHasher.getAllBucketsToMembersMapping();
		for (Map.Entry<String, List<String>> e : map.entrySet()) {
			if (e.getValue().size() != 0) {
				coordinator = e.getKey();
				break;
			}
		}
		
		return coordinator;
	}
	
	public static List<String> getPreferenceList(List<String> servers) throws InterruptedException {
		
		ConsistentHasher<String, String> cHasher = getConsistentHasher();
		List<String> sortedBuckets = new ArrayList<String>();
		
		for (String s : servers) {
			cHasher.addBucket(s);
		}
		
		cHasher.addMember("a");
		Map<String, List<String>> map;
		while (cHasher.getAllBuckets().size() != 0) {
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
}
