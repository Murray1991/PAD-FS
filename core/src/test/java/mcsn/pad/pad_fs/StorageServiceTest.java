package mcsn.pad.pad_fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
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
import mcsn.pad.pad_fs.membership.MembershipService;
import mcsn.pad.pad_fs.message.ClientMessage;
import mcsn.pad.pad_fs.message.Message;
import mcsn.pad.pad_fs.message.client.GetMessage;
import mcsn.pad.pad_fs.message.client.ListMessage;
import mcsn.pad.pad_fs.message.client.PutMessage;
import mcsn.pad.pad_fs.storage.StorageService;
import mcsn.pad.pad_fs.storage.local.HashMapStore;
import mcsn.pad.pad_fs.storage.local.LocalStore;
import mcsn.pad.pad_fs.utils.TestUtils;
import voldemort.versioning.Versioned;

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
		System.out.println("-- wait for startup");
		Thread.sleep(6000);
	}
	
	@After
	public void teardown() {
		TestUtils.shutdownServices(mServices);
		TestUtils.shutdownServices(sServices);
		TestUtils.checkValues(map, lStores);
	}
	
	@Test
	public void testWithoutUpdates() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		
		System.out.println("-- deliver messages to the system");
		map = TestUtils.createMessages(Message.PUT, TestUtils.createKeys(num));
		TestUtils.deliverMessages(map, sServices);
		System.out.println("-- wait...");
		Thread.sleep(20000);
		
	}

	@Test
	public void testWithUpdates() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		
		testWithoutUpdates();
		System.out.println("-- update some values");
		updateAndDeliverMessages(map, sServices);
		System.out.println("-- wait");
		Thread.sleep(20000);
		
	}
	
	@Test
	public void testWithFailuresAndUpdates() throws FileNotFoundException, JSONException, IOException, InterruptedException {
		
		testWithoutUpdates();
		System.out.println("-- shutdown member service of " + sServices.get(0)); 
		mServices.get(0).shutdown();
		IService removed = mServices.remove(0);
		System.out.println("-- wait");
		Thread.sleep(10000);
		
		/* each service here should know dim-2 members */
		checkIfCorrect(mServices, dim-2);
		System.out.println("-- update some values");
		updateAndDeliverMessages(map, sServices);
		System.out.println("-- wait");
		Thread.sleep(5000);
		
		System.out.println("-- restart " + sServices.get(0));
		removed.start();
		mServices.add(removed);
		System.out.println("-- wait");
		Thread.sleep(20000);
		checkIfCorrect(mServices, dim-1);
	}
	
	@Test
	public void concurrencyAndRemoveTest() throws InterruptedException, FileNotFoundException, JSONException, IOException {
		
		testWithoutUpdates();
		
		System.out.println("-- shutdown all member services");
		TestUtils.shutdownServices(mServices);
		Thread.sleep(10000);
		
		/* each service should know nobody */
		checkIfCorrect(mServices, 0);
		
		System.out.println("-- put different values with same key in two different nodes");
		List<Serializable> keys = TestUtils.createKeys(30);
		List<Map<Serializable, ClientMessage>> list = new ArrayList<>();
		for (int idx = 0; idx < 2; idx++) {				
			Map<Serializable, ClientMessage> msgs = TestUtils.createMessages(Message.PUT, keys);
			for (Serializable key : msgs.keySet()) {
				StorageService ss = (StorageService) sServices.get(idx);
				ClientMessage res = ss.deliverMessage(msgs.get(key));
				Assert.assertTrue(res.status == Message.SUCCESS);
			}
			list.add(msgs);
		}
		
		System.out.println("-- restart all the member services");
		TestUtils.startServices(mServices);
		System.out.println("-- wait");
		Thread.sleep(20000);
		
		/* each service should know dim-1 members */
		checkIfCorrect(mServices, dim-1);
		
		System.out.println("-- get \"concurrent\" messages");
		Map<Serializable, ClientMessage> getMessages = TestUtils.createMessages(Message.GET, keys);
		for (int i=0; i<2; i++) {
			
			for (Entry<Serializable, ClientMessage> e : getMessages.entrySet()) {
				
				int idx = Math.abs(e.hashCode()) % dim;
				StorageService ss = (StorageService) sServices.get(idx);
				GetMessage res = (GetMessage) ss.deliverMessage(e.getValue());
				
				Assert.assertTrue(res.status == Message.SUCCESS);
				Assert.assertNotNull(res.values);
				
				PutMessage sentMsg = (PutMessage) list.get(i).get(e.getKey());
				byte[] exp = sentMsg.value.getValue();
				byte[] res1 = res.values.get(0).getValue();
				byte[] res2 = res.values.get(1).getValue();
				Assert.assertTrue( 
						new String(exp).substring(0,5) + " -- " 
						+ new String(res1).substring(0, 5) + " -- "
						+ new String(res2).substring(0, 5),
						Arrays.equals(exp, res1) || Arrays.equals(exp, res2));
				
			}
		}
		
		Random rand = new Random();
		keys.removeIf( k -> rand.nextInt(10) < 5);
		
		System.out.println("-- remove some keys...");
		Map<Serializable, ClientMessage> rmMessages = TestUtils.createMessages(Message.REMOVE, keys);
		for (Entry<Serializable, ClientMessage> e : rmMessages.entrySet()) {
			int idx = Math.abs(e.hashCode()) % dim;
			StorageService ss = (StorageService) sServices.get(idx);
			ClientMessage res = ss.deliverMessage(e.getValue());
			Assert.assertTrue(res.status == Message.SUCCESS);
			
			/* check values for that key in the stores */
			for (int i=0; i<dim; i++) {
				LocalStore ls = lStores.get(i);
				List<Versioned<byte[]>> values = ls.get(e.getKey());
				Assert.assertTrue(values == null || values.get(0).getValue() == null);
			}
		}
		
		System.out.println("-- try to get the removed keys");
		getMessages = TestUtils.createMessages(Message.GET, keys);
		for (Entry<Serializable, ClientMessage> e : getMessages.entrySet()) {
			int idx = rand.nextInt(dim);
			StorageService ss = (StorageService) sServices.get(idx);
			ClientMessage res = ss.deliverMessage(e.getValue());
			Assert.assertTrue(res.status == Message.NOT_FOUND);
		}
	}
	
	@Test
	public void testList() throws InterruptedException, FileNotFoundException, JSONException, IOException {
		
		testWithoutUpdates();
		
		System.out.println("-- send list message");
		ListMessage msg = new ListMessage();
		int idx = Math.abs(msg.hashCode()) % dim;
		
		StorageService ss = (StorageService) sServices.get(idx);
		ListMessage res = (ListMessage) ss.deliverMessage(msg);
		Assert.assertTrue(res.keys.size() == num);
	}
	
	private void updateAndDeliverMessages(Map<Serializable, ClientMessage> map, List<IService> sServices) {
		Random rand = new Random();
		int bound = sServices.size();
		for (Serializable key : map.keySet()) {
			if (rand.nextInt(10) <= 3) {
				int idx = rand.nextInt(bound);
				
				ClientMessage msg = new PutMessage(key, TestUtils.getRandomString().getBytes());
				map.put(key, msg);
				
				StorageService ss = (StorageService) sServices.get(idx);
				ss.deliverMessage(msg);
			}
		}
	}
	
	private void checkIfCorrect(List<IService> services, int expected) {
		for (IService service : services) {
			MembershipService ms = (MembershipService) service;
			int size = ms.getMembers().size();
			Assert.assertTrue("size: " + size , size == expected);
		}
	}
}
