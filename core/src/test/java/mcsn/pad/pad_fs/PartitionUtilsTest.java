package mcsn.pad.pad_fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.json.JSONException;
import org.junit.Test;

import it.cnr.isti.hpclab.consistent.ConsistentHasher;
import junit.framework.Assert;
import mcsn.pad.pad_fs.membership.Member;
import mcsn.pad.pad_fs.membership.PartitionUtils;
import mcsn.pad.pad_fs.utils.TestUtils;

public class PartitionUtilsTest {

	int numServers = 10;
	int numKeys = 1000;
	
	List<String> createKeys(int number) {
		Random random = new Random(System.currentTimeMillis());
		List<String> keys = new ArrayList<>();
		for (int i=0; i < number; i++)
			keys.add(TestUtils.nextSessionId(random));
		return keys;
	}
	
	void printLoadDistribution(ConsistentHasher<String, String> cHasher) {
		Map<String, List<String>> map = cHasher.getAllBucketsToMembersMapping();
		for (Map.Entry<String, List<String>> entry : map.entrySet()) {
			System.out.println(entry.getKey() + " : ");
			System.out.println("\t"+entry.getValue().size());
		}
	}
	
	@Test
	public void cHasherTest() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		List<Node> nodes = TestUtils.getMembers(numServers);	//buckets
		List<String> keys = createKeys(numKeys);				//members
		
		ConsistentHasher<Member, String> cHasher = PartitionUtils
				.getConsistentHasher();
		
		for (Node node : nodes)
			cHasher.addBucket(node.getMyself());
		for (String key : keys)
			cHasher.addMember(key);
		
		System.out.println("print bucket distribution for three different values of size of virtual nodes");
		PartitionUtils.printBucketDistribution(cHasher);
		
		String key = "abcdefgh";
		cHasher.addMember(key);
		
		Member coordinator = PartitionUtils
				.getCoordinator(cHasher.getAllBuckets(), key);
		
		List<Member> prefList = PartitionUtils
				.getPreferenceList(cHasher.getAllBuckets(), key, 5);
		
		System.out.println("Preference list, asterisk next to the coordinator for key " + key);
		for (Member s : prefList)
			System.out.println(s + (s.equals(coordinator) ? " *" : ""));
		
		System.out.printf("\n");
		
		while (cHasher.getAllBuckets().size() >= 3 && ! prefList.isEmpty()) {
			int next = (prefList.indexOf(coordinator) + 1) % prefList.size();
			Member expected = prefList.get(next);
			
			System.out.println("remove coordinator " + coordinator + ", expected new is " + expected);
			cHasher.removeBucket(coordinator);
			prefList.remove(coordinator);
			
			coordinator = PartitionUtils
					.getCoordinator(cHasher.getAllBuckets(), key);
			
			if (! prefList.isEmpty()) {
				Assert.assertTrue("coordinator " + coordinator + " not expected", 
						coordinator.equals(expected));
			}
		}
		
		System.out.printf("\n");
		
		System.out.println("Preference list, asterisk next to the coordinator for key " + key);
		for (Member s : prefList)
			System.out.println(s + (s.equals(coordinator) ? " *" : ""));
	}

}
