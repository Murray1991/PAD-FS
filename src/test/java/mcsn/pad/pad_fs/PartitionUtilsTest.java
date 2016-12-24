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
import mcsn.pad.pad_fs.partitioning.PartitionUtils;
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
	public void test() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		
		List<Node> nodes = TestUtils.getMembers(numServers);
		List<String> keys = createKeys(numKeys);
		
		ConsistentHasher<String, String> cHasher = PartitionUtils.getConsistentHasher();
		for (Node node : nodes)
			cHasher.addBucket(node.getMyself().host);
		
		for (String key : keys)
			cHasher.addMember(key);
		
		String key = "abcdefgh";
		cHasher.addMember(key);
		String coordinator = PartitionUtils.getCoordinator(cHasher.getAllBuckets(), key);
		List<String> prefList = PartitionUtils.getPreferenceList(cHasher.getAllBuckets());
		
		int prev_index = prefList.indexOf(coordinator);
		int index = prefList.indexOf(coordinator);
		
		while (cHasher.getAllBuckets().size() >= 3) {
			System.out.println("coordinator: " + coordinator + " in " + index);
			System.out.println("distribution:");
			printLoadDistribution(cHasher);
			
			System.out.println("---------------");
			
			prev_index = prefList.indexOf(coordinator);
			cHasher.removeBucket(coordinator);
			
			coordinator = PartitionUtils.getCoordinator(cHasher.getAllBuckets(), key);
			index = prefList.indexOf(coordinator);
			
			Assert.assertTrue("WTF", (prev_index+1) % prefList.size() == index );
		}
		
		System.out.println("coordinator: " + coordinator + " in " + index);
		System.out.println("distribution:");
		printLoadDistribution(cHasher);
		System.out.println("---------------");
	}

}
