package mcsn.pad.pad_fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import mcsn.pad.pad_fs.storage.local.LocalStore;
import mcsn.pad.pad_fs.utils.DummyLocalStore;
import mcsn.pad.pad_fs.utils.TestUtils;
import voldemort.versioning.Occurred;
import voldemort.versioning.VectorClock;

public class StorageServiceTest {
	
	private int dim = 4;
	private int num = 500;
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
			LocalStore localStore = new DummyLocalStore("127.0.0"+i);
			mServices.add( membershipService );
			lStores.add( localStore );
			sServices.add( new StorageService(membershipService, localStore));
		}
		
		TestUtils.startServices(mServices);
		TestUtils.startServices(sServices);
		
		map = createMessages(num);
		Thread.sleep(5000);
	}
	
	@After
	public void teardown() {
		TestUtils.shutdownServices(mServices);
		TestUtils.shutdownServices(sServices);

		checkValues(map, lStores);

		for (LocalStore ls : lStores)
			Assert.assertTrue("size: " + ls.size(), ls.size() == num);
	}
	
	@Test
	public void testWithoutUpdates() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		
		System.out.println("====== testWithoutUpdates ======");

		System.out.println("-- deliver Messages to the Services");
		deliverMessages(map, sServices);
		
		System.out.println("-- wait...");
		Thread.sleep(15000);
	}

	@Test
	public void testWithUpdates() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		
		System.out.println("====== testWithUpdates ======");

		System.out.println("-- deliver Messages to the Services");
		deliverMessages(map, sServices);
		
		System.out.println("-- wait...");
		Thread.sleep(10000);
		
		System.out.println("-- update values");
		updateAndDeliverMessages(map, sServices);
		
		System.out.println("-- wait...");
		Thread.sleep(10000);
		
	}
	
	@Test
	public void testWithFailuresAndUpdates() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		
		System.out.println("====== testWithFailuresAndUpdates ======");
		
		System.out.println("-- deliver Messages to the Services");
		deliverMessages(map, sServices);
		
		System.out.println("-- wait...");
		Thread.sleep(10000);
		
		System.out.println("-- interrupt a service in order to simulate a temporary failure"); 
		mServices.get(0).shutdown();
		
		Thread.sleep(10000);
		
		System.out.println("-- update values");
		updateAndDeliverMessages(map, sServices);
		
		System.out.println("-- wait...");
		Thread.sleep(10000);
		
		System.out.println("-- restart the interrupted service"); 
		mServices.get(0).start();
		
		System.out.println("-- Wait...");
		Thread.sleep(15000);
	}
	
	private Map<Serializable, ClientMessage> createMessages(int num) {
		Map<Serializable, ClientMessage> map = new HashMap<>();
		for (int i=0; i<num; i++) {
			ClientMessage msg = new ClientMessage(Message.PUT, TestUtils.getRandomString(), 
					TestUtils.getVersioned(TestUtils.getRandomString().getBytes()));
			map.put(msg.key, msg);
		}
		return map;
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
