package mcsn.pad.pad_fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;
import mcsn.pad.pad_fs.common.Configuration;
import mcsn.pad.pad_fs.common.IService;
import mcsn.pad.pad_fs.common.Utils;
import mcsn.pad.pad_fs.membership.MembershipService;
import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.storage.StorageService;
import mcsn.pad.pad_fs.storage.local.HashMapStore;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import mcsn.pad.pad_fs.utils.TestUtils;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;

public class StorageServiceTest {
	
	private int dim = 4;
	private int num = 300;
	private List<IService> mServices;
	private List<IService> sServices;
	private List<LocalStore> lStores;
	Map<Serializable, ClientMessage> map;
	
	@Before
	public void setup() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		String filename = this.getClass().getResource("/gossip.conf").getFile();
		File configFile = new File(filename);
		mServices = new ArrayList<>();
		sServices = new ArrayList<>();
		lStores = new ArrayList<>();
		for (int i = 1 ; i <= dim; i++) {
			Configuration config = new Configuration("127.0.0."+i, configFile);
			MembershipService membershipService = new MembershipService(config);
			LocalStore localStore = new HashMapStore("127.0.0"+i);
			mServices.add( membershipService );
			lStores.add( localStore );
			sServices.add( new StorageService(membershipService, localStore));
		}
		
		TestUtils.startServices(mServices);
		TestUtils.startServices(sServices);
		
		map = createMessages(Message.PUT, createKeys(num));
		Thread.sleep(5000);
	}
	
	@After
	public void teardown() {
		TestUtils.shutdownServices(mServices);
		TestUtils.shutdownServices(sServices);
		checkValues(map, lStores);
	}
	
	@Test
	public void testWithoutUpdates() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		System.out.println("====== testWithoutUpdates ======");
		deliverMessages(map, sServices);
		System.out.println("-- wait...");
		Thread.sleep(20000);
	}

	@Test
	public void testWithUpdates() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		System.out.println("====== testWithUpdates ======");
		deliverMessages(map, sServices);
		System.out.println("-- wait...");
		Thread.sleep(10000);
		System.out.println("-- update some values...");
		updateAndDeliverMessages(map, sServices);
		System.out.println("-- wait...");
		Thread.sleep(20000);
		
	}
	
	@Test
	public void testWithFailuresAndUpdates() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		System.out.println("====== testWithFailuresAndUpdates ======");
		deliverMessages(map, sServices);
		System.out.println("-- wait...");
		Thread.sleep(10000);
		System.out.println("-- disable gossipService of " + sServices.get(0)); 
		mServices.get(0).shutdown();
		System.out.println("-- wait...");
		Thread.sleep(10000);
		System.out.println("-- update some values...");
		updateAndDeliverMessages(map, sServices);
		System.out.println("-- wait...");
		Thread.sleep(5000);
		System.out.println("-- restart the interrupted service"); 
		mServices.get(0).start();
		System.out.println("-- wait...");
		Thread.sleep(20000);
	}
	
	@Test
	public void concurrencyAndRemoveTest() throws InterruptedException {
		System.out.println("====== concurrencyAndRemoveTest ======");
		deliverMessages(map, sServices);
		System.out.println("-- wait...");
		Thread.sleep(6000);
		
		System.out.println("-- shutdown all the gossip services...");
		TestUtils.shutdownServices(mServices);
		Thread.sleep(6000);
		
		System.out.println("-- put different values with same key in two different nodes...");
		List<Serializable> keys = createKeys(30);
		List<Map<Serializable, ClientMessage>> list = new ArrayList<>();
		Random rand = new Random();
		for (int i=0; i<2; i++) {
			final Map<Serializable, ClientMessage> msgs = createMessages(Message.PUT, keys);
			final int idx = rand.nextInt(dim);
			list.add(msgs);
			new Thread( () -> {
				for (Serializable key : msgs.keySet()) {
					StorageService ss = (StorageService) sServices.get(idx);
					ss.deliverMessage(msgs.get(key));
				}
			}).start();
		}
		
		Thread.sleep(2000);
		
		System.out.println("-- restart all the gossip services...");
		TestUtils.startServices(mServices);
		
		System.out.println("-- wait...");
		Thread.sleep(25000);
		
		//concurrency test here
		Map<Serializable, ClientMessage> getMessages = createMessages(Message.GET, keys);
		for (int i=0; i<2; i++) {
			for (Entry<Serializable, ClientMessage> e : getMessages.entrySet()) {
				int idx = rand.nextInt(dim);
				ClientMessage res = 
						((StorageService) sServices.get(idx))
						.deliverMessage(e.getValue());
				byte[] exp = list.get(i).get(e.getKey()).value.getValue();
				byte[] res1 = res.value.getValue();
				Assert.assertNotNull(res.values);
				byte[] res2 = res.values.get(0).getValue();
				Assert.assertTrue( "something is wrong... ",
						Arrays.equals(exp, res1) || Arrays.equals(exp, res2));
			}
		}
		
		//select the keys to remove
		for (int i = 0; i < keys.size()/2 ; i++) {
			int idx = rand.nextInt(keys.size());
			keys.remove(idx);
		}
		
		System.out.println("-- remove some keys...");
		final Map<Serializable, ClientMessage> removeMessages = createMessages(Message.REMOVE, keys);
		for (Entry<Serializable, ClientMessage> e : removeMessages.entrySet()) {
			int idx = rand.nextInt(dim);
			ClientMessage res = ((StorageService) sServices.get(idx))
					.deliverMessage(e.getValue());
			Assert.assertTrue(res.status == Message.SUCCESS);
		}
		
		System.out.println("-- try to get the removed keys...");
		getMessages = createMessages(Message.GET, keys);
		for (Entry<Serializable, ClientMessage> e : getMessages.entrySet()) {
			int idx = rand.nextInt(dim);
			ClientMessage res = 
					((StorageService) sServices.get(idx))
					.deliverMessage(e.getValue());
			Assert.assertTrue("status: "+res.status, res.status == Message.NOT_FOUND);
		}
	}
	
	private Map<Serializable, ClientMessage> createMessages(int type, List<Serializable> keys) {
		Map<Serializable, ClientMessage> map = new HashMap<>();
		for (Serializable key : keys)
			switch (type) {
			case Message.PUT:
				map.put(key, new ClientMessage(type, key, 
						TestUtils.getVersioned(TestUtils.getRandomString().getBytes())));
				break;
			case Message.GET:
			case Message.REMOVE:
				map.put(key, new ClientMessage(type, key, true));
			default:
				break;
			}
		return map;
	}
	
	private List<Serializable> createKeys(int num) {
		List<Serializable> keys = new ArrayList<>();
		for (int i=0; i<num; i++)
			keys.add(TestUtils.getRandomString());
		return keys;
	}
	
	private void deliverMessages(Map<Serializable, ClientMessage> map, List<IService> sServices) {
		Iterator<Serializable> it = map.keySet().iterator();
		while (it.hasNext()) {
			ClientMessage msg = map.get(it.next());
			int idx = (Math.abs(msg.key.hashCode()) % sServices.size()) + 1;
			((StorageService) sServices.get(idx-1)).deliverMessage(msg);
		}
	}
	
	private void updateAndDeliverMessages(Map<Serializable, ClientMessage> map, List<IService> sServices) {
		Random rand = new Random();
		for (Serializable key : map.keySet()) {
			if (rand.nextInt(10) <= 3) {
				ClientMessage msg = 
						new ClientMessage(
							Message.PUT, 
							key, 
							TestUtils.getVersioned(
									TestUtils.getRandomString().getBytes()
									)
						);
				map.put(key, msg);
				int idx = (Math.abs(msg.key.hashCode()) % sServices.size()) + 1;
				((StorageService) sServices.get(idx-1)).deliverMessage(msg);
			}
		}
	}
	
	private void checkValues(Map<Serializable, ClientMessage> map, List<LocalStore> lStores){
		for (Serializable key : map.keySet() ) {
			
			int stores = TestUtils.countKey(key, lStores);
			Assert.assertTrue( stores + " != " + lStores.size(), stores == lStores.size() );
			
			Assert.assertTrue( 
					"[" + new String(lStores.get(0).get(key).getValue()).substring(0, 5) + " -- " + lStores.get(0).get(key).getVersion() + "] ;; " + 
							"[" + new String(lStores.get(1).get(key).getValue()).substring(0, 5) + lStores.get(1).get(key).getVersion()  + "] ;;"  +
							"[" + new String(lStores.get(2).get(key).getValue()).substring(0, 5) + lStores.get(2).get(key).getVersion()  + "] ;; " +
							"[" + new String(lStores.get(3).get(key).getValue()).substring(0, 5) + lStores.get(3).get(key).getVersion()  + "] ;;" +
							"[" + new String(map.get(key).value.getValue()).substring(0, 5) + map.get(key).value.getVersion() + "] ;;" +
							Utils.compare(map.get(key).value, lStores.get(3).get(key)) ,
					TestUtils.checkValues( 
							TestUtils.getValues(key, lStores), 
							lStores.get(0).get(key)
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
	
    public void testComparisons() throws InterruptedException {
        Assert.assertTrue("The empty clock should not happen before itself.",
                   TestUtils.getClock().compare(TestUtils.getClock()) != Occurred.CONCURRENTLY);
        Assert.assertTrue("A clock should not happen before an identical clock.",
                   TestUtils.getClock(1, 1, 2).compare(TestUtils.getClock(1, 1, 2)) != Occurred.CONCURRENTLY);
        Assert.assertTrue(" A clock should happen before an identical clock with a single additional event.",
                   TestUtils.getClock(1, 1, 2).compare(TestUtils.getClock(1, 1, 2, 3)) == Occurred.BEFORE);
        Assert.assertTrue("Clocks with different events should be concurrent.",
                   TestUtils.getClock(1).compare(TestUtils.getClock(2)) == Occurred.CONCURRENTLY);
        Assert.assertTrue("Clocks with different events should be concurrent.",
                   TestUtils.getClock(1, 1, 2).compare(TestUtils.getClock(1, 1, 3)) == Occurred.CONCURRENTLY);
        Assert.assertTrue("Clocks with different events should be concurrent.",
                   TestUtils.getClock(1, 2, 3, 3).compare(TestUtils.getClock(1, 1, 2, 3)) == Occurred.CONCURRENTLY);
        Assert.assertTrue(TestUtils.getClock(2, 2).compare(TestUtils.getClock(1, 2, 2, 3)) == Occurred.BEFORE
                   && TestUtils.getClock(1, 2, 2, 3).compare(TestUtils.getClock(2, 2)) == Occurred.AFTER);
        VectorClock vc1 = TestUtils.getClock(1);
        System.out.println(vc1);
        VectorClock vc2 = TestUtils.getClock(2);
        System.out.println(vc2);
        vc1.merge(vc2);
        System.out.println(vc1);
	}
}
