package mcsn.pad.pad_fs.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import org.json.JSONException;

import it.cnr.isti.hpclab.consistent.ConsistentHasher;
import it.cnr.isti.hpclab.consistent.ConsistentHasherImpl;
import junit.framework.Assert;
import mcsn.pad.pad_fs.Node;
import mcsn.pad.pad_fs.common.IService;
import mcsn.pad.pad_fs.common.Utils;
import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.storage.local.HashMapStore;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Versioned;

public class TestUtils {
	
	/**
     * getClock(1,1,2,2,2) means a clock that has two writes on node 1 and 3
     */
    public static VectorClock getClock(int... nodes) {
        VectorClock clock = new VectorClock();
        increment(clock, nodes);
        return clock;
    }
    
    /**
     * Record events for the given sequence of nodes
     */
    public static void increment(VectorClock clock, int... nodes) {
        for(int n: nodes)
            clock.incrementVersion((short) n, clock.getTimestamp());
    }
    
    /**
     * Return Versioned byte array with a sequence of node events
     */
    public static Versioned<byte[]> getVersioned(byte[] value, int... nodes) {
        return new Versioned<byte[]>(value, getClock(nodes));
    }
    
    /**
     * Return Versioned byte array without the sequence of node events
     */
    public static Versioned<byte[]> getVersioned(byte[] value) {
        return new Versioned<byte[]>(value);
    }
    
    /**
     * Return Versioned Integer object
     */
    public static Versioned<Integer> getVersioned(Integer value, int... versionIncrements) {
        return new Versioned<Integer>(value, getClock(versionIncrements));
    }
    
    /**
     * It is used for the randomString() method
     */
    private static Random random;
    
    /**
     * Return random String object
     */
    public static String getRandomString() {
    	if (random == null)
    		random = new Random();
    	return new BigInteger(1000, random).toString(32);
    }
    
    /**
     * Start Services
     */
    public static void startServices(List<IService> services) {
    	for (IService service : services)
    		service.start();
    }
    
    /**
     * Shutdown Services
     */
    public static void shutdownServices(List<IService> services) {
    	for (IService service : services)
    		service.shutdown();
    }
    
    /**
     * Tell if the key exists in at least one of the Local Stores
     */
    public static boolean existKey (Serializable key, List<LocalStore> lStores) {
		return countKey (key, lStores) != 0;
	}
    
    /**
     * Count in how many Local Stores the key is stored
     */
    public static int countKey (Serializable key, List<LocalStore> lStores) {
		int count = 0;
		for ( LocalStore l : lStores )
			count  += (l.get(key) != null && l.get(key).get(0).getValue() != null ? 1 : 0);
		return count;
	}
    
    /**
     * Return an iterable collection of values for each local store
     */
    public static Iterable<Versioned<byte[]>> getValues (Serializable key, List<LocalStore> lStores) {
		Vector<Versioned<byte[]>> values = new Vector<Versioned<byte[]>>();
		for ( LocalStore l : lStores )
			values.addAll(l.get(key));
		return values;
	}
    
    /**
     * Check if the content of versioned items is equal to the expected one
     */
    public static boolean checkValues (Iterable<Versioned<byte[]>> values, Versioned<byte[]> expected) {
		Iterator<Versioned<byte[]>> itValue = values.iterator();
		while (itValue.hasNext()) {
			Versioned<byte[]> curr = itValue.next();
			if ( ! Arrays.equals(expected.getValue(), curr.getValue()) ) {
				System.err.println("Expected value is different!!");
				return false;
			}
		}
		return true;
	}

	public static String nextSessionId(Random random) {
	    return new BigInteger(100, random).toString(32);
	}
	
	public static ClientMessage randomMessage(ClientMessage msg) {
		Versioned<byte[]> value = new Versioned<byte[]>(nextSessionId(new SecureRandom()).getBytes());
		return new ClientMessage(msg.type, msg.key, value);
	}
	
	public static ClientMessage randomMessage(int type) {
		String key = TestUtils.nextSessionId(new SecureRandom()); 
		Versioned<byte[]> value = new Versioned<byte[]>(nextSessionId(new SecureRandom()).getBytes());
		return new ClientMessage(type, key, value);
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
			members.add(i-1,  new Node("127.0.0."+i, configFile, HashMapStore.class));
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

	public static List<ClientMessage> getMessages(int type, int dim) {
		List<ClientMessage> list = new ArrayList<>();
		for (int i = 0; i < dim; i++)
			list.add(randomMessage(type));
		return list;
	}
	
	public static void checkValues(Map<Serializable, ClientMessage> map, List<LocalStore> lStores){
		for (Serializable key : map.keySet() ) {
			
			int stores = TestUtils.countKey(key, lStores);
			Assert.assertTrue( stores + " != " + lStores.size(), stores == lStores.size() );
			
			Assert.assertTrue( 
					
					"[" + new String(getSingleValue(key, lStores.get(0))).substring(0, 5) + " -- " + lStores.get(0).get(key).get(0).getVersion() + "] ;; " + 
							"[" + new String(getSingleValue(key, lStores.get(1))).substring(0, 5) + lStores.get(1).get(key).get(0).getVersion()  + "] ;;"  +
							"[" + new String(getSingleValue(key, lStores.get(2))).substring(0, 5) + lStores.get(2).get(key).get(0).getVersion()  + "] ;; " +
							"[" + new String(getSingleValue(key, lStores.get(3))).substring(0, 5) + lStores.get(3).get(key).get(0).getVersion()  + "] ;;" +
							"[" + new String(map.get(key).value.getValue()).substring(0, 5) + map.get(key).value.getVersion() + "] ;;" +
							Utils.compare(map.get(key).value, lStores.get(3).get(key).get(0)) ,
							
					TestUtils.checkValues( 
							TestUtils.getValues(key, lStores), 
							lStores.get(0).get(key).get(0)
							) 
					);
			
			Assert.assertTrue( 
					TestUtils.checkValues( 
							TestUtils.getValues(key, lStores), 
							map.get(key).value
							) 
					); 
		}
	}
	
	private static byte[] getSingleValue(Serializable key, LocalStore ls) {
		List<Versioned<byte[]>> vv = ls.get(key);
		Assert.assertTrue(vv.size() == 1);
		return vv.get(0).getValue();
	}
}
