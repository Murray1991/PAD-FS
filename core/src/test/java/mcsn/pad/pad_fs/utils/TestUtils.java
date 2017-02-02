package mcsn.pad.pad_fs.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import org.json.JSONException;

import junit.framework.Assert;
import mcsn.pad.pad_fs.Node;
import mcsn.pad.pad_fs.common.IService;
import mcsn.pad.pad_fs.common.Utils;
import mcsn.pad.pad_fs.membership.MembershipService;
import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.message.client.GetMessage;
import mcsn.pad.pad_fs.message.client.PutMessage;
import mcsn.pad.pad_fs.message.client.RemoveMessage;
import mcsn.pad.pad_fs.storage.StorageService;
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
	
	/**
	 * get a list of random elements
	 */
	public static List<Versioned<byte[]>> getElements(int dim) {
		Random random = new Random(System.currentTimeMillis());
		ArrayList<Versioned<byte[]>> elements = new ArrayList<>();
		for (int i=0; i<dim; i++) {
			elements.add( new Versioned<byte[]>( nextSessionId(random).getBytes()) );
		}
		return elements;
	}
	
	/**
	 * get list of dim nodes from gossip.conf file
	 */
	public static List<Node> getMembers(int dim) throws FileNotFoundException, JSONException, IOException, InterruptedException {
		String filename = TestUtils.class.getResource("/gossip.conf").getFile();
		File configFile = new File(filename);
		List<Node> members = new ArrayList<>();
		for (int i = 1; i <= dim; i++) {
			members.add(i-1,  new Node("127.0.0."+i, configFile, HashMapStore.class));
		}
		return members;
	}
	
	public static void deliverMessages(Map<Serializable, ClientMessage> map, List<IService> sServices) {
		
		Iterator<Serializable> it = map.keySet().iterator();
		Random rand = new Random();
		int bound = sServices.size();
		
		while (it.hasNext()) {
			ClientMessage msg = map.get(it.next());
			int idx = rand.nextInt(bound);
			
			StorageService ss = (StorageService) sServices.get(idx);
			ClientMessage res = ss.deliverMessage(msg);
			Assert.assertTrue("status: " + res.status, res.status == Message.SUCCESS);
		}
		
	}
	
	public static Map<Serializable, ClientMessage> createMessages(int type, List<Serializable> keys) {
		
		Map<Serializable, ClientMessage> map = new HashMap<>();
		
		for (Serializable key : keys)
			switch (type) {
			case Message.PUT:
				map.put(key, new PutMessage(key, TestUtils.getRandomString().getBytes()));
				break;
			case Message.GET:
				map.put(key, new GetMessage(key));
				break;
			case Message.REMOVE:
				map.put(key, new RemoveMessage(key));
			default:
				break;
			}
		
		return map;
	}
	
	public static void checkIfCorrect(List<IService> services, int expected) {
		for (IService service : services) {
			MembershipService ms = (MembershipService) service;
			Assert.assertTrue(ms.getMembers().size() == expected);
		}
	}
	
	public static List<Serializable> createKeys(int num) {
		List<Serializable> keys = new ArrayList<>();
		for (int i=0; i<num; i++)
			keys.add(TestUtils.getRandomString());
		return keys;
	}
	
	public static void checkValues(Map<Serializable, ClientMessage> map, List<LocalStore> lStores){
		for (Serializable key : map.keySet() ) {
			int stores = TestUtils.countKey(key, lStores);
			
			Assert.assertTrue( stores + " != " + lStores.size(), stores == lStores.size() );
			Assert.assertTrue( checkValuesPrintInfo(key, map, lStores),
					TestUtils.checkValues( 
							TestUtils.getValues(key, lStores), 
							lStores.get(0).get(key).get(0)
							) 
					);
			
			Assert.assertTrue( 
					TestUtils.checkValues( 
							TestUtils.getValues(key, lStores), 
							((PutMessage) map.get(key)).value
							) 
					); 
		}
	}
	
	private static String checkValuesPrintInfo(Serializable key, Map<Serializable, ClientMessage> map, List<LocalStore> lStores) {
		
		String info = "";
		for (LocalStore ls : lStores) {
			info += "[";
			List<Versioned<byte[]>> lvv = ls.get(key);
			Assert.assertTrue(lvv.size() == 1);
			Versioned<byte[]> vv = lvv.get(0);
			info += new String(vv.getValue()).substring(0, 5);
			info += " -- ";
			info += vv.getVersion();
			info += "] ;; ";
		}
		
		/* I Know that is a PushMessage! */
		Versioned<byte[]> vv = ((PutMessage) map.get(key)).value;
		info += "[";
		info += new String(vv.getValue()).substring(0,5);
		info += " -- ";
		info += vv.getVersion();
		info += "] ... ";
		
		info += Utils.compare(vv, lStores.get(3).get(key).get(0));
			
		return info;
	}
	
	public static ClientMessage randomMessage(ClientMessage msg) {
		Versioned<byte[]> value = new Versioned<byte[]>(nextSessionId(new SecureRandom()).getBytes());
		Serializable key = ((PutMessage) msg).key;
		return new PutMessage(key, value);
	}
}
